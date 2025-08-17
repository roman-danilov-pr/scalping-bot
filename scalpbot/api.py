# scalpbot/api.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from scalpbot.shared_state import get_state

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

state = get_state()

@app.get("/status")
def status():
    s = state.snapshot()
    return {"uptime": s['uptime'], "n_signals": len(s['signals']), "open_positions": len(s['open_positions']), "metrics": s['metrics']}

@app.get("/signals")
def signals(n: int = 200):
    return state.get_signals(n)

@app.get("/positions")
def positions():
    snap = state.snapshot()
    return {"open": snap['open_positions'], "closed": snap['closed_positions']}

@app.get("/logs")
def logs(n: int = 200):
    return state.tail_logs(n)

if __name__ == "__main__":
    uvicorn.run("scalpbot.api:app", host="127.0.0.1", port=8080, reload=False)
