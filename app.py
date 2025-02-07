from fastapi import FastAPI, Request, HTTPException
import os
import logging
import requests
import aiohttp
import asyncio  # Added this import
from typing import Dict, Any

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Required environment variables
required_env_vars = [
    "TELEGRAM_SERVICE_URL",
    "AI_SIGNAL_SERVICE_URL",
    "NEWS_AI_SERVICE_URL",
    "SUBSCRIBER_MATCHER_URL"
]

# Validate environment variables
for var in required_env_vars:
    if not os.getenv(var):
        raise RuntimeError(f"Missing required environment variable: {var}")

async def distribute_signal(signal):
    """Distribute signal to all services"""
    try:
        logger.info(f"Starting to distribute signal: {signal}")
        
        # Get service URLs from environment
        telegram_url = os.getenv('TELEGRAM_SERVICE_URL')
        ai_signal_url = os.getenv('AI_SIGNAL_SERVICE_URL')
        news_ai_url = os.getenv('NEWS_AI_SERVICE_URL')
        subscriber_matcher_url = os.getenv('SUBSCRIBER_MATCHER_URL')
        
        logger.info(f"Using Telegram URL: {telegram_url}")
        
        # Send to all services concurrently
        async with aiohttp.ClientSession() as session:
            tasks = []
            
            # Send to AI Signal Service
            tasks.append(session.post(
                f"{ai_signal_url}/analyze",
                json=signal
            ))
            
            # Send to News AI Service
            tasks.append(session.post(
                f"{news_ai_url}/analyze",
                json=signal
            ))
            
            # Send to Subscriber Matcher
            tasks.append(session.post(
                f"{subscriber_matcher_url}/match",
                json=signal
            ))
            
            # Send to Telegram Service
            tasks.append(session.post(
                f"{telegram_url}/send",
                json=signal
            ))
            
            # Wait for all responses
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process responses
            results = {}
            for resp in responses:
                if isinstance(resp, Exception):
                    logger.error(f"Error in service call: {str(resp)}")
                else:
                    logger.info(f"Service response status: {resp.status}")
                    if resp.status == 200:
                        results[resp.url.path] = await resp.json()
                    else:
                        error_text = await resp.text()
                        logger.error(f"Service error: {error_text}")
            
            return results

    except Exception as e:
        logger.error(f"Error distributing signal: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/signal")
async def process_signal(signal: Dict[Any, Any]):
    try:
        # Distribute signal to all services
        results = await distribute_signal(signal)
        return {
            "status": "success",
            "results": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "signal-processor",
        "dependencies": {
            "telegram": bool(os.getenv("TELEGRAM_SERVICE_URL")),
            "ai_signal": bool(os.getenv("AI_SIGNAL_SERVICE_URL")),
            "news_ai": bool(os.getenv("NEWS_AI_SERVICE_URL")),
            "subscriber_matcher": bool(os.getenv("SUBSCRIBER_MATCHER_URL"))
        }
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 5000))
    logger.info(f"Starting signal processor on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
