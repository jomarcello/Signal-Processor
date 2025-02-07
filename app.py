from fastapi import FastAPI, Request, HTTPException
import os
import logging
import requests
import aiohttp
import asyncio
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

def get_full_url(url: str, endpoint: str = "") -> str:
    """Ensure URL starts with https:// and has correct endpoint"""
    if not url.startswith(('http://', 'https://')):
        url = f"https://{url}"
    return f"{url.rstrip('/')}/{endpoint.lstrip('/')}"

async def distribute_signal(signal):
    """Distribute signal to all services"""
    try:
        logger.info(f"Starting to distribute signal: {signal}")
        
        # Get service URLs from environment and ensure they have https://
        telegram_url = get_full_url(os.getenv('TELEGRAM_SERVICE_URL'), 'send')
        ai_signal_url = get_full_url(os.getenv('AI_SIGNAL_SERVICE_URL'), 'analyze')
        news_ai_url = get_full_url(os.getenv('NEWS_AI_SERVICE_URL'), 'analyze')
        subscriber_matcher_url = get_full_url(os.getenv('SUBSCRIBER_MATCHER_URL'), 'match')
        
        logger.info(f"Using URLs: Telegram={telegram_url}, AI={ai_signal_url}, News={news_ai_url}, Matcher={subscriber_matcher_url}")
        
        # Send to subscriber matcher first to get chat_ids
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(subscriber_matcher_url, json=signal) as resp:
                    if resp.status == 200:
                        subscriber_data = await resp.json()
                        chat_ids = subscriber_data.get('matched_subscribers', [])
                        signal['chat_ids'] = [sub['chat_id'] for sub in chat_ids]
                    else:
                        error_text = await resp.text()
                        logger.error(f"Subscriber matcher error: {error_text}")
            except Exception as e:
                logger.error(f"Error calling subscriber matcher: {str(e)}")
            
            # Now send to other services with chat_ids
            tasks = []
            
            # Send to AI Signal Service
            tasks.append(session.post(
                ai_signal_url,
                json=signal,
                ssl=False
            ))
            
            # Send to News AI Service
            tasks.append(session.post(
                news_ai_url,
                json=signal,
                ssl=False
            ))
            
            # Send to Telegram Service
            tasks.append(session.post(
                telegram_url,
                json=signal,
                ssl=False
            ))
            
            # Wait for all responses
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process responses
            results = {}
            for i, resp in enumerate(responses):
                if isinstance(resp, Exception):
                    logger.error(f"Error in service call {i}: {str(resp)}")
                    results[f"service_{i}"] = {"error": str(resp)}
                else:
                    try:
                        logger.info(f"Service {i} response status: {resp.status}")
                        text = await resp.text()
                        logger.info(f"Service {i} response: {text}")
                        if resp.status == 200:
                            results[resp.url.path] = await resp.json()
                        else:
                            logger.error(f"Service {i} error: {text}")
                            results[resp.url.path] = {"error": text}
                    except Exception as e:
                        logger.error(f"Error processing response {i}: {str(e)}")
                        results[f"service_{i}"] = {"error": str(e)}
            
            return results

    except Exception as e:
        logger.error(f"Error distributing signal: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/signal")
async def process_signal(signal: Dict[Any, Any]):
    """Process incoming trading signal"""
    try:
        # Validate required fields
        required_fields = ["symbol", "action", "price", "stopLoss", "takeProfit", "interval"]
        for field in required_fields:
            if field not in signal:
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")
        
        # Distribute signal to all services
        results = await distribute_signal(signal)
        return {
            "status": "success",
            "results": results
        }
    except Exception as e:
        logger.error(f"Error processing signal: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint that verifies all service connections"""
    health_status = {
        "status": "healthy",
        "service": "signal-processor",
        "dependencies": {}
    }
    
    # Check all service URLs
    for service, env_var in {
        "telegram": "TELEGRAM_SERVICE_URL",
        "ai_signal": "AI_SIGNAL_SERVICE_URL",
        "news_ai": "NEWS_AI_SERVICE_URL",
        "subscriber_matcher": "SUBSCRIBER_MATCHER_URL"
    }.items():
        url = os.getenv(env_var)
        if not url:
            health_status["dependencies"][service] = "missing_url"
            health_status["status"] = "degraded"
        else:
            try:
                url = get_full_url(url, "health")
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, ssl=False) as response:
                        if response.status == 200:
                            health_status["dependencies"][service] = "healthy"
                        else:
                            health_status["dependencies"][service] = f"unhealthy_{response.status}"
                            health_status["status"] = "degraded"
            except Exception as e:
                health_status["dependencies"][service] = f"error_{str(e)}"
                health_status["status"] = "degraded"
    
    return health_status

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 5000))
    logger.info(f"Starting signal processor on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
