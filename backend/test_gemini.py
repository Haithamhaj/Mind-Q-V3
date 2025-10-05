#!/usr/bin/env python3
"""
اختبار تكامل Gemini API
"""

import os
import sys
sys.path.append('.')

from app.services.bi.llm_client import LLMClient

def test_gemini():
    """اختبار Gemini API"""
    
    print("اختبار Gemini API...")
    
    # إنشاء client مع Gemini
    client = LLMClient(provider="gemini")
    
    # اختبار prompt بسيط
    test_prompt = """
أنت محلل بيانات خبير. حلل السؤال التالي وحوّله إلى structured query.

السؤال: ما متوسط وقت الشحن لشركة DHL؟

أعد JSON فقط:
{
  "intent": "aggregate",
  "entities": {"metric": "transit_time", "dimension": "carrier"},
  "filters": {"carrier": "DHL"},
  "aggregation": "mean",
  "language": "ar"
}
"""
    
    try:
        print("إرسال الطلب للـ Gemini...")
        response = client.call(test_prompt, max_tokens=500)
        
        print("تم استلام الرد من Gemini!")
        print(f"الرد: {response[:200]}...")
        
        # فحص إذا كان الرد يحتوي على JSON
        if "{" in response and "}" in response:
            print("الرد يحتوي على JSON - ممتاز!")
        else:
            print("الرد لا يحتوي على JSON صالح")
            
        return True
        
    except Exception as e:
        print(f"خطأ في اختبار Gemini: {e}")
        return False

def test_all_providers():
    """اختبار جميع مقدمي الخدمة"""
    
    providers = ["gemini", "anthropic", "openai"]
    
    for provider in providers:
        print(f"\nاختبار {provider}...")
        
        client = LLMClient(provider=provider)
        
        try:
            response = client.call("Test prompt", max_tokens=100)
            print(f"{provider}: يعمل")
        except Exception as e:
            print(f"{provider}: خطأ - {e}")

if __name__ == "__main__":
    print("بدء اختبار LLM Integration...")
    print("=" * 50)
    
    # اختبار Gemini تحديداً
    gemini_works = test_gemini()
    
    print("\n" + "=" * 50)
    print("اختبار جميع المقدمين...")
    test_all_providers()
    
    print("\n" + "=" * 50)
    if gemini_works:
        print("Gemini يعمل بنجاح!")
        print("تذكر إضافة GEMINI_API_KEY في ملف .env")
    else:
        print("Gemini يحتاج إعداد API Key")
        print("احصل على API Key من: https://makersuite.google.com/app/apikey")
