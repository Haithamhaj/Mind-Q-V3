from fastapi import APIRouter, HTTPException, Body
from typing import List
import json
from datetime import datetime

from app.services.phase14_5_llm_analysis import LLMAnalysisService
from app.services.llm.client import LLMConfigurationError, get_llm_client
from app.services.llm.prompts import SYSTEM_PROMPT, CHAT_PROMPT_TEMPLATE
from app.models.phase14_5_result import Phase14_5Result, ChatMessage, DecisionLogEntry
from app.config import settings


router = APIRouter(prefix="/llm-analysis", tags=["llm-analysis"])


@router.post("/run-analysis", response_model=Phase14_5Result)
async def run_llm_analysis():
    try:
        eval_report_path = settings.artifacts_dir / "evaluation_report.json"
        if not eval_report_path.exists():
            raise HTTPException(status_code=400, detail="Phase 14 not complete. Run model training first.")

        service = LLMAnalysisService()
        result = service.run()
        return result
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/chat", response_model=ChatMessage)
async def chat_with_llm(message: str = Body(..., embed=True), language: str = Body("en", embed=True)):
    try:
        artifacts_dir = settings.artifacts_dir
        context_parts = []

        eval_path = artifacts_dir / "evaluation_report.json"
        if eval_path.exists():
            with open(eval_path, "r", encoding="utf-8") as f:
                eval_data = json.load(f)
                context_parts.append(f"**Evaluation Report:**\n{json.dumps(eval_data, indent=2)}")

        feat_path = artifacts_dir / "feature_importance.json"
        if feat_path.exists():
            with open(feat_path, "r", encoding="utf-8") as f:
                feat_data = json.load(f)
                context_parts.append(f"**Feature Importance:**\n{json.dumps(feat_data, indent=2)}")

        insights_path = artifacts_dir / "llm_insights_report.json"
        if insights_path.exists():
            with open(insights_path, "r", encoding="utf-8") as f:
                insights_data = json.load(f)
                context_parts.append(f"**Previous Insights:**\n{json.dumps(insights_data, indent=2)}")

        context = "\n\n".join(context_parts)
        prompt = CHAT_PROMPT_TEMPLATE.format(context=context, question=message)
        if language == "ar":
            prompt += "\n\nIMPORTANT: Respond in Arabic."

        try:
            llm = get_llm_client()
            response_text = llm.call(prompt, system=SYSTEM_PROMPT)
        except LLMConfigurationError as exc:
            raise HTTPException(
                status_code=503,
                detail=f"LLM is not configured: {exc}",
            )

        chat_message = ChatMessage(
            role="assistant",
            content=response_text,
            timestamp=datetime.utcnow().isoformat(),
            artifacts_referenced=["evaluation_report.json", "feature_importance.json"],
        )
        return chat_message
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/log-decision")
async def log_decision(
    decision_type: str = Body(...), description: str = Body(...), rationale: str = Body(...)
):
    try:
        log_path = settings.artifacts_dir / "decision_log.json"
        if log_path.exists():
            with open(log_path, "r", encoding="utf-8") as f:
                log_data = json.load(f)
        else:
            log_data = []

        new_entry = DecisionLogEntry(
            timestamp=datetime.utcnow().isoformat(),
            decision_type=decision_type,
            description=description,
            made_by="human",
            rationale=rationale,
        )
        log_data.append(new_entry.dict())

        with open(log_path, "w", encoding="utf-8") as f:
            json.dump(log_data, f, indent=2, ensure_ascii=False)

        return {"message": "Decision logged successfully", "entry": new_entry}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/recommendations", response_model=List[dict])
async def get_recommendations():
    try:
        recs_path = settings.artifacts_dir / "recommendations.json"
        if not recs_path.exists():
            raise HTTPException(status_code=404, detail="Recommendations not found. Run Phase 14.5 first.")
        with open(recs_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Recommendations not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/executive-summary")
async def get_executive_summary():
    try:
        insights_path = settings.artifacts_dir / "llm_insights_report.json"
        if not insights_path.exists():
            raise HTTPException(status_code=404, detail="Insights not found. Run Phase 14.5 first.")
        with open(insights_path, "r", encoding="utf-8") as f:
            insights_data = json.load(f)
        return {
            "executive_summary": insights_data.get("executive_summary"),
            "key_findings": insights_data.get("key_findings", []),
            "next_steps": insights_data.get("next_steps", []),
        }
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Insights not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


