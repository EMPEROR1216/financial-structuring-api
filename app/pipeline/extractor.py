import requests
import json
import re

OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "tinyllama"

def ai_extract(content):
    prompt = f"""
Extract these fields from the financial document:

date
vendor
amount
tax

Return ONLY valid JSON.

Document:
{content}
"""

    try:
        response = requests.post(
            OLLAMA_URL,
            json={
                "model": MODEL,
                "prompt": prompt,
                "stream": False
            }
        )

        data = response.json()
        text = data.get("response", "").strip()

        # Extract JSON safely
        json_match = re.search(r"\{.*\}", text, re.DOTALL)
        if json_match:
            return json.loads(json_match.group())

    except Exception as e:
        print("AI extraction failed:", e)

    return None


def fallback_extract(content, filename):
    # When AI fails, return a minimal structured record. Use empty strings instead
    # of 'unknown' to keep outputs clean; downstream normalization in the
    # aggregator will enforce final format.
    base_name = filename.split('.')[0] if filename else ""
    return {
        "date": "",
        "vendor": base_name,
        "amount": 0,
        "tax": 0,
        "source": filename or ""
    }


def extract_data(filename, content=""):
    ai_result = ai_extract(content)

    if ai_result:
        ai_result["source"] = filename
        return ai_result

    return fallback_extract(content, filename)