# Phase 12: Future Enhancements

This MVP implementation includes only basic text analysis. Below are planned enhancements.

## Phase 12.2: Enhanced (Topic Modeling + Keywords)

**Estimated Time:** +8 hours

### Topic Modeling (LDA)
- Library: `gensim==4.3.2`
- Extract 3-5 topics per text column
- Output: Top 10 keywords per topic
- File: `phase12/topics.py`

### Keyword Extraction
- Methods: TF-IDF + RAKE
- Libraries: `scikit-learn`, `rake-nltk==1.0.6`
- Output: Top 20 keywords per column
- File: `phase12/keywords.py`

## Phase 12.3: Pro (NER + Clustering)

**Estimated Time:** +10 hours

### Named Entity Recognition
- English: `spacy==3.7.2` + `en_core_web_sm`
- Arabic: `camel-tools==1.5.2` (400MB download)
- Entities: PERSON, ORG, GPE, DATE
- File: `phase12/ner.py`

### Text Clustering
- Method: KMeans on TF-IDF vectors
- Auto-determine k (2-5 clusters)
- Output: Cluster assignments + centroids
- File: `phase12/clustering.py`

## Implementation Notes

1. **Add phase parameter to API:**
```python
phase: str = Form("mvp")  # "mvp", "enhanced", "pro"
```

2. **Memory guards:**
- Enhanced: n <= 500k
- Pro: n <= 100k

3. **Fallback strategy:**
- Always implement simple fallback if library fails
- Example: If spaCy fails → use regex patterns for NER

4. **Arabic support:**
- All advanced features should support Arabic
- Use CAMeL Tools for Arabic NLP
- Maintain RTL support in UI

## API Enhancement Example

```python
@router.post("/phase12-text-features", response_model=Phase12Result)
async def run_phase12(
    phase: str = Form("mvp")  # "mvp", "enhanced", "pro"
):
    """
    Phase 12: Text Features with configurable complexity
    
    - mvp: Basic features + sentiment (current)
    - enhanced: + topic modeling + keywords
    - pro: + NER + clustering
    """
    try:
        # Load data
        data_path = settings.artifacts_dir / "merged_data.parquet"
        if not data_path.exists():
            data_path = settings.artifacts_dir / "train.parquet"
        
        if not data_path.exists():
            raise HTTPException(400, "No data found. Run previous phases first.")
        
        df = pd.read_parquet(data_path)
        
        # Initialize orchestrator with phase parameter
        orchestrator = Phase12Orchestrator(df=df, phase=phase)
        result = orchestrator.run(settings.artifacts_dir)
        
        return result
    
    except Exception as e:
        raise HTTPException(500, str(e))
```

## Future Dependencies

```txt
# Phase 12.2 Enhanced
rake-nltk==1.0.6
wordcloud==1.9.2

# Phase 12.3 Pro
spacy==3.7.2
camel-tools==1.5.2  # 400MB download for Arabic NLP
```

## File Structure (Future)

```
backend/app/services/phase12/
├── __init__.py
├── detection.py          # Current MVP
├── basic_features.py     # Current MVP
├── sentiment_simple.py   # Current MVP
├── orchestrator.py       # Current MVP (enhanced)
├── topics.py            # Phase 12.2
├── keywords.py          # Phase 12.2
├── ner.py               # Phase 12.3
├── clustering.py        # Phase 12.3
└── FUTURE_ENHANCEMENTS.md
```

## Testing Strategy

Each enhancement phase should include:
1. Unit tests for new functionality
2. Integration tests with existing MVP
3. Performance tests for memory constraints
4. Fallback tests for missing dependencies

## UI Integration

Future frontend components:
- `TopicViewer.tsx` - Display topics and keywords
- `NerViewer.tsx` - Show extracted entities
- `ClusteringViewer.tsx` - Visualize text clusters

## Performance Considerations

1. **Memory Management:**
   - Process text in chunks for large datasets
   - Use streaming for very large text columns

2. **Caching:**
   - Cache processed text features
   - Store model artifacts for reuse

3. **Parallel Processing:**
   - Process multiple text columns in parallel
   - Use multiprocessing for CPU-intensive tasks

## Migration Path

1. **Phase 12.1 (MVP):** ✅ Complete
2. **Phase 12.2 (Enhanced):** Add topic modeling and keywords
3. **Phase 12.3 (Pro):** Add NER and clustering
4. **Phase 12.4 (Enterprise):** Add custom models and fine-tuning

Each phase maintains backward compatibility and graceful degradation.
