import logging
import os
import requests

logger = logging.getLogger("voice-chat-bot")

def get_current_model(exo_url="http://192.168.20.2:52415"):
    try:
        state = requests.get(f"{exo_url}/state").json()
        for iid, inst in state.get("instances", {}).items():
            inner = inst.get("MlxRingInstance", {})
            model_id = inner.get("shardAssignments", {}).get("modelId")
            if model_id:
                logger.info("resolved LLM model from exo state: %s", model_id)
                return model_id
    except Exception as exc:
        logger.warning("failed to query exo state for model: %s", exc)
    fallback = os.getenv("LLM_MODEL", "mlx-community/GLM-4.7-Flash-5bit")
    logger.info("using fallback LLM model: %s", fallback)
    return fallback
