#!/usr/bin/env python3
"""
Multi-Agent Gateway - Run multiple DurableAgents in a single process
Each agent gets its own Dapr API token for isolation
"""

import os
import asyncio
import logging
from dotenv import load_dotenv

# Agent configurations
AGENT_CONFIGS = {
    "elfapp": {
        "name": "Legolas",
        "role": "Elf",
        "goal": "Act as a scout and protector with keen senses",
        "instructions": [
            "Speak with grace and wisdom",
            "Use superior vision to scout ahead",
            "Excel in ranged combat",
        ],
    },
    # "hobbitapp": {
    #     "name": "Frodo", 
    #     "role": "Hobbit",
    #     "goal": "Carry the Ring and navigate danger",
    #     "instructions": [
    #         "Speak with humility and determination",
    #         "Endure hardships and stay true to the mission",
    #         "Seek guidance from allies",
    #     ],
    # },
    # "wizardapp": {
    #     "name": "Gandalf",
    #     "role": "Wizard", 
    #     "goal": "Guide with wisdom and strategy",
    #     "instructions": [
    #         "Speak with wisdom and mystery",
    #         "Provide strategic counsel",
    #         "Use magic sparingly but effectively",
    #     ],
    # },
}


async def create_agent_with_token(agent_app_id, config, token):
    """Create a DurableAgent with its own Dapr token"""
    print(f"🚀 Creating {config['name']} with token isolation...")
    
    # Save current environment
    original_env = {
        "DAPR_API_TOKEN": os.environ.get("DAPR_API_TOKEN"),
        "DAPR_APP_ID": os.environ.get("DAPR_APP_ID"),
    }
    
    try:
        # Set agent-specific environment
        os.environ["DAPR_API_TOKEN"] = token
        os.environ["DAPR_APP_ID"] = agent_app_id
        
        # Import here to pick up the environment
        from dapr_agents import DurableAgent
        
        # Create agent with its isolated token
        # No broadcast topic for headless agents to avoid conflicts
        agent = DurableAgent(
            name=config["name"],
            role=config["role"],
            goal=config["goal"],
            instructions=config["instructions"],
            message_bus_name="messagepubsub",
            state_store_name="workflowstatestore",
            state_key=f"workflow_state_{agent_app_id}",
            agents_registry_store_name="agentstatestore",
            agents_registry_key="agents_registry",
            # No broadcast_topic_name - agents only respond to direct messages
        )
        
        print(f"✅ Created {config['name']} with App ID: {agent_app_id}")
        return agent
        
    finally:
        # Restore original environment
        for key, value in original_env.items():
            if value is not None:
                os.environ[key] = value
            elif key in os.environ:
                del os.environ[key]


async def main():
    """Main function to run all agents"""
    print("🌉 Multi-Agent Gateway Starting...")
    
    # Get agent names from environment
    agent_names_str = os.getenv("AGENT_NAMES", "")
    if not agent_names_str:
        print("❌ No AGENT_NAMES environment variable found")
        return
    
    agent_names = [name.strip().lower() for name in agent_names_str.split(",")]
    print(f"📋 Configured agents: {agent_names}")
    
    # Create agents with their tokens
    agents = []
    for agent_name in agent_names:
        # Get agent-specific token
        token_key = f"DAPR_API_TOKEN_{agent_name.upper()}"
        token = os.getenv(token_key)
        
        if not token:
            print(f"⚠️ No token found for {agent_name}, skipping...")
            continue
        
        # Get config
        if agent_name not in AGENT_CONFIGS:
            print(f"⚠️ No configuration for {agent_name}, skipping...")
            continue
        
        config = AGENT_CONFIGS[agent_name]
        
        # Create agent with isolated token
        try:
            agent = await create_agent_with_token(agent_name, config, token)
            agents.append(agent)
        except Exception as e:
            print(f"❌ Failed to create {agent_name}: {e}")
            import traceback
            traceback.print_exc()
    
    if not agents:
        print("❌ No agents created successfully")
        return
    
    print(f"🚀 Starting {len(agents)} agents...")
    
    # Start all agents concurrently
    try:
        await asyncio.gather(
            *[agent.start() for agent in agents]
        )
    except Exception as e:
        print(f"❌ Error running agents: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    load_dotenv()
    logging.basicConfig(level=logging.INFO)
    
    # Check if we're in multi-agent mode
    if os.getenv("MULTI_AGENT_MODE") != "true":
        print("⚠️ Not in multi-agent mode, exiting...")
        exit(1)
    
    print("🎯 Multi-Agent Gateway Mode Activated")
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Gateway shutting down...")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
