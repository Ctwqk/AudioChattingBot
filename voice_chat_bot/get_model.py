import os
import requests

def get_current_model(exo_url="http://10.0.0.128:52415"):
    try:
        state = requests.get(f"{exo_url}/state").json()
        for iid, inst in state.get("instances", {}).items():
            inner = inst.get("MlxRingInstance", {})
            model_id = inner.get("shardAssignments", {}).get("modelId")
            if model_id:
                return model_id
    except:
        pass
    return os.getenv("LLM_MODEL", "mlx-community/GLM-4.7-Flash-5bit")