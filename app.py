from fastapi import FastAPI, Request, HTTPException
import aiohttp
import logging
import os
from typing import Dict, Any

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

async def distribute_signal(signal):
    """Distribute signal to all services"""
    try:
        logger.info(f"Starting to distribute signal: {signal}")
        
        # Send to Telegram Service
        logger.info("Sending to Telegram Service...")
        telegram_url = os.environ.get('TELEGRAM_SERVICE_URL', 'http://tradingview-telegram-service:5000')
        logger.info(f"Using Telegram URL: {telegram_url}")
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{telegram_url}/send_signal",
                json=signal
            ) as response:
                logger.info(f"Telegram response status: {response.status}")
                if response.status == 200:
                    response_data = await response.json()
                    logger.info(f"Telegram response: {response_data}")
                    return response_data
                else:
                    error_text = await response.text()
                    logger.error(f"Telegram error: {error_text}")
                    raise HTTPException(status_code=500, detail=error_text)

    except Exception as e:
        logger.error(f"Error distributing signal: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/signal")
async def process_signal(signal: Dict[Any, Any]):
    try:
        # Forward to telegram service
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "http://tradingview-telegram-service:5000/send_signal",
                json=signal
            ) as response:
                if response.status != 200:
                    raise HTTPException(status_code=500, detail=await response.text())
                return await response.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 5000))
    logger.info(f"Starting signal processor on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port) 