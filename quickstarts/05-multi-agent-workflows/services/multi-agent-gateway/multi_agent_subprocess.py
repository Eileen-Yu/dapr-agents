#!/usr/bin/env python3
"""
Multi-Agent Subprocess - Each agent runs in its own subprocess
This provides true isolation while still being managed by a single parent process
"""

import os
import sys
import asyncio
import subprocess
from dotenv import load_dotenv


async def run_agent_subprocess(agent_id):
    """Run a single agent in a subprocess"""
    prefix = agent_id.upper()
    
    # Check if configuration exists
    grpc_endpoint = os.getenv(f"DAPR_GRPC_ENDPOINT_{prefix}")
    if not grpc_endpoint:
        print(f"❌ No configuration for {agent_id}")
        return None
    
    print(f"🚀 Launching {agent_id} in subprocess...")
    
    # Create environment for subprocess
    env = os.environ.copy()
    
    # Map agent-specific env vars to standard names
    env["DAPR_APP_ID"] = os.getenv(f"DAPR_APP_ID_{prefix}", agent_id)
    env["DAPR_GRPC_ENDPOINT"] = os.getenv(f"DAPR_GRPC_ENDPOINT_{prefix}")
    env["DAPR_HTTP_ENDPOINT"] = os.getenv(f"DAPR_HTTP_ENDPOINT_{prefix}", "")
    env["DAPR_API_TOKEN"] = os.getenv(f"DAPR_API_TOKEN_{prefix}", "")
    env["AGENT_NAME"] = agent_id
    
    # Python script to run in subprocess
    agent_code = f'''
import asyncio
import logging
from dapr_agents import DurableAgent

CONFIGS = {{
    "elfagent": {{
        "name": "Legolas",
        "role": "Elf",
        "goal": "Act as a scout and protector with keen senses",
        "instructions": [
            "Speak with grace and wisdom",
            "Use superior vision to scout ahead",
            "Excel in ranged combat",
        ],
    }},
    "hobbitagent": {{
        "name": "Frodo",
        "role": "Hobbit",
        "goal": "Carry the Ring and navigate danger",
        "instructions": [
            "Speak with humility and determination",
            "Endure hardships and stay true to the mission",
            "Seek guidance from allies",
        ],
    }},
    "wizardagent": {{
        "name": "Gandalf",
        "role": "Wizard",
        "goal": "Guide with wisdom and strategy",
        "instructions": [
            "Speak with wisdom and mystery",
            "Provide strategic counsel",
            "Use magic sparingly but effectively",
        ],
    }},
}}

async def main():
    agent_id = "{agent_id}"
    config = CONFIGS.get(agent_id)
    
    print(f"🏃 Starting {{config['name']}} ({{agent_id}})", flush=True)
    
    agent = DurableAgent(
        name=config["name"],
        role=config["role"],
        goal=config["goal"],
        instructions=config["instructions"],
        message_bus_name="messagepubsub",
        state_store_name="workflowstatestore",
        state_key=f"workflow_state_{{config['name'].lower()}}",
        agents_registry_store_name="agentstatestore",
        agents_registry_key="agents_registry",
        broadcast_topic_name="beacon_channel",
    )
    
    print(f"✅ {{config['name']}} running", flush=True)
    await agent.start()

logging.basicConfig(level=logging.INFO)
asyncio.run(main())
'''
    
    # Start subprocess
    process = await asyncio.create_subprocess_exec(
        sys.executable, "-c", agent_code,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT
    )
    
    return process, agent_id


async def monitor_subprocess(process, agent_id):
    """Monitor subprocess output"""
    async for line in process.stdout:
        print(f"[{agent_id}] {line.decode().rstrip()}")
    
    await process.wait()
    return process.returncode


async def main():
    """Main function"""
    print("🎯 Multi-Agent Subprocess Runner", flush=True)
    print("=" * 60, flush=True)
    print("📦 Each agent runs in its own subprocess with isolated environment", flush=True)
    
    agent_names_str = os.getenv("AGENT_NAMES", "")
    print(f"📋 AGENT_NAMES from env: '{agent_names_str}'", flush=True)
    
    if not agent_names_str:
        print("❌ No AGENT_NAMES environment variable", flush=True)
        return
    
    agent_names = [n.strip().lower() for n in agent_names_str.split(",")]
    print(f"📋 Parsed agents: {agent_names}", flush=True)
    
    # Launch all agents in subprocesses
    subprocesses = []
    for agent_name in agent_names:
        result = await run_agent_subprocess(agent_name)
        if result:
            subprocesses.append(result)
            # Brief pause between launches
            await asyncio.sleep(1)
    
    if not subprocesses:
        print("❌ No agents launched")
        return
    
    print(f"✅ Launched {len(subprocesses)} agent subprocesses")
    print("📡 Each agent has its own environment and sidecar")
    print("=" * 60)
    
    # Monitor all subprocesses
    try:
        tasks = [monitor_subprocess(proc, agent_id) for proc, agent_id in subprocesses]
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        print("\n👋 Terminating subprocesses...")
        for proc, _ in subprocesses:
            proc.terminate()
            await proc.wait()


if __name__ == "__main__":
    load_dotenv()
    
    print("=" * 60)
    print("MULTI-AGENT SUBPROCESS RUNNER")
    print("True isolation via subprocesses")
    print("=" * 60)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Shutting down...")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()