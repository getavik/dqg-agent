import google.generativeai as genai
import os

_cached_model = None

def _get_available_model(api_key: str):
    global _cached_model
    if _cached_model:
        return _cached_model

    genai.configure(api_key=api_key)
    
    # Prioritize flash models for speed, then other generative models
    preferred_models = ["models/gemini-2.5-pro", "models/gemini-2.5-flash", "models/gemini-pro-latest"]
    
    for model_name in preferred_models:
        try:
            model = genai.GenerativeModel(model_name)
            if model.count_tokens and model.generate_content: # Check for essential capabilities
                _cached_model = model # Cache the found model
                return model
        except Exception:\
            continue # Try next model if current one fails

    # Fallback to any model that supports generateContent
    for m in genai.list_models():
        if "generateContent" in m.supported_generation_methods:
            try:
                model = genai.GenerativeModel(m.name)
                _cached_model = model
                return model
            except Exception:
                continue

    return None
