#!/usr/bin/env python3
"""
Multi-Agent Single Process - Run multiple DurableAgents in a single process
Each agent gets its own Dapr API token for isolation while sharing the same process
NO ENVIRONMENT VARIABLE SWAPPING - Using hardcoded credentials throughout
"""

import os
import sys
import asyncio
import logging
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv

# IMMEDIATE OUTPUT to confirm script is starting
print(f"\n{'='*60}", flush=True)
print(f"🚀 MULTI-AGENT SINGLE PROCESS STARTING", flush=True)
print(f"   PID: {os.getpid()}", flush=True)
print(f"   Python: {sys.executable}", flush=True)
print(f"   Script: {__file__}", flush=True)
print(f"{'='*60}\n", flush=True)

# CRITICAL: Skip health check for remote Diagrid endpoints
# We're not using local sidecars, so health checks will fail
print("🚫 DISABLING DAPR HEALTH CHECKS (using remote endpoints)", flush=True)
try:
    from dapr.clients.health import DaprHealth
    # Override the health check to do nothing
    DaprHealth.wait_until_ready = staticmethod(lambda: None)
    print("✅ Health checks disabled successfully", flush=True)
except Exception as e:
    print(f"⚠️  Could not disable health checks: {e}", flush=True)

# Agent configurations - CLI uses 'agent' suffix
AGENT_CONFIGS = {
    "elfagent": {
        "name": "Legolas",
        "role": "Elf",
        "goal": "Act as a scout and protector with keen senses",
        "instructions": [
            "Speak like Legolas, with grace, wisdom, and keen observation.",
            "Use your superior vision to scout ahead and detect danger before others.",
            "Excel in ranged combat with your bow - never miss your mark.",
            "Move silently through any terrain, whether forest, mountain, or city.",
            "Always show loyalty to the Fellowship and respect for all living things.",
        ],
    },
    "hobbitagent": {
        "name": "Frodo", 
        "role": "Hobbit",
        "goal": "Carry the Ring and navigate danger",
        "instructions": [
            "Speak like Frodo, with humility, determination, and occasional weariness from your burden.",
            "Bear the weight of the One Ring with courage, though it grows heavier with each step.",
            "Show compassion even to enemies, remembering Gandalf's words about pity.",
            "Endure great hardships but never lose sight of why the mission matters.",
            "Seek guidance from Sam and your companions when the path seems dark.",
        ],
    },
    "wizardagent": {
        "name": "Gandalf",
        "role": "Wizard", 
        "goal": "Guide with wisdom and strategy",
        "instructions": [
            "Speak like Gandalf, with wisdom, patience, and a touch of mystery.",
            "Guide the Fellowship with both gentle counsel and firm direction when needed.",
            "Use your vast knowledge of Middle-earth's history and lore to inform decisions.",
            "Resort to magic only when necessary, preferring wisdom and persuasion.",
            "Show both kindness to the small and defiance to the mighty who abuse their power.",
        ],
    },
}

# Global dictionary to store agent credentials and runtime components
AGENT_CREDENTIALS = {}
AGENT_RUNTIMES = {}  # Will store custom runtime components per agent


class HardcodedCredentialsAgent:
    """
    A wrapper that creates DurableAgent with hardcoded credentials
    WITHOUT any environment variable manipulation
    """
    def __init__(self, agent_name: str, config: Dict, credentials: Dict):
        self.agent_name = agent_name
        self.config = config
        self.credentials = credentials
        self.agent = None
        
    async def create(self):
        """Create the agent with hardcoded credentials"""
        from dapr_agents import DurableAgent
        from dapr.clients.grpc.client import DaprGrpcClient
        from dapr.clients.grpc.interceptors import DaprClientInterceptor
        
        print(f"\n{'='*60}", flush=True)
        print(f"🔧 CREATING AGENT: {self.config['name']} ({self.agent_name})", flush=True)
        print(f"{'='*60}", flush=True)
        print(f"📌 Hardcoded Credentials:", flush=True)
        print(f"   Token: {self.credentials['token'][:30]}...{self.credentials['token'][-10:]}", flush=True)
        print(f"   Full Token Length: {len(self.credentials['token'])} chars", flush=True)
        print(f"   Endpoint: {self.credentials['grpc_endpoint']}", flush=True)
        print(f"   App ID: {self.credentials.get('app_id', 'N/A')}", flush=True)
        
        # Create agent-specific DaprClient with hardcoded token
        print(f"\n🔌 Creating DaprClient for {self.config['name']}:", flush=True)
        print(f"   Using token: ...{self.credentials['token'][-10:]}", flush=True)
        print(f"   Using endpoint: {self.credentials['grpc_endpoint']}", flush=True)
        
        interceptors = [DaprClientInterceptor([('dapr-api-token', self.credentials['token'])])]
        
        # Create the client (health check already disabled globally)
        agent_dapr_client = DaprGrpcClient(
            address=self.credentials['grpc_endpoint'],
            interceptors=interceptors
        )
        print(f"   ✓ DaprClient created with hardcoded token", flush=True)
        
        # Store the client for the agent
        AGENT_RUNTIMES[self.agent_name] = {
            'dapr_client': agent_dapr_client,
            'token': self.credentials['token'],
            'endpoint': self.credentials['grpc_endpoint']
        }
        
        # Store credentials in closure variables for the inner class
        agent_token = self.credentials['token']
        agent_endpoint = self.credentials['grpc_endpoint']
        agent_key = self.agent_name
        
        # Create a custom DurableAgent that will use hardcoded credentials
        class HardcodedDurableAgent(DurableAgent):
            """Custom agent that stores and uses hardcoded credentials"""
            
            def model_post_init(self, __context: Any) -> None:
                """Override to ensure we use hardcoded credentials"""
                print(f"\n🎯 Initializing {self.name}'s WorkflowRuntime & WorkflowClient:", flush=True)
                # Use closure variables instead of instance attributes
                print(f"   Agent Key: {agent_key}", flush=True)
                print(f"   Token for Runtime: ...{agent_token[-10:]}", flush=True)
                print(f"   Endpoint for Runtime: {agent_endpoint}", flush=True)
                
                # Set custom workflow runtime parameters as private attributes
                # The base class will check for these and use them
                # agent_endpoint is like: https://grpc-prj1223451.api.dev.cloud.diagrid.io:443
                # We've updated the SDK to handle full URLs properly
                self._wf_host = agent_endpoint  # Pass the full endpoint URL
                self._wf_port = None  # Not needed for full URLs
                self._wf_token = agent_token
                
                print(f"   🔧 Setting WorkflowRuntime parameters:", flush=True)
                print(f"      _wf_host: {self._wf_host}", flush=True)
                print(f"      _wf_port: {self._wf_port}", flush=True)
                print(f"      _wf_token: ...{agent_token[-10:]}", flush=True)
                print(f"   📦 These will be passed to WorkflowRuntime & DaprWorkflowClient", flush=True)
                
                print(f"   🏗️ Calling super().model_post_init() to create WorkflowRuntime...", flush=True)
                # Let parent initialize with custom parameters
                super().model_post_init(__context)
                
                print(f"   ✅ WorkflowRuntime created for {self.name}", flush=True)
                if hasattr(self, 'wf_runtime') and self.wf_runtime:
                    print(f"      - wf_runtime: {self.wf_runtime}", flush=True)
                    # Try to access internal worker to verify endpoint
                    if hasattr(self.wf_runtime, '_WorkflowRuntime__worker'):
                        worker = self.wf_runtime._WorkflowRuntime__worker
                        print(f"      - Runtime's internal worker configured", flush=True)
                if hasattr(self, 'wf_client') and self.wf_client:
                    print(f"      - wf_client: {self.wf_client}", flush=True)
                    # Try to access internal client to verify endpoint  
                    if hasattr(self.wf_client, '_DaprWorkflowClient__obj'):
                        print(f"      - Client's internal client configured", flush=True)
                
                print(f"   ✓ {self.name} fully initialized with hardcoded credentials (token ...{agent_token[-10:]})", flush=True)
                print(f"{'='*60}\n", flush=True)
        
        # Create the agent with hardcoded credentials
        print(f"\n🏗️ Creating HardcodedDurableAgent instance:", flush=True)
        print(f"   Name: {self.config['name']}", flush=True)
        print(f"   Role: {self.config['role']}", flush=True)
        print(f"   State Key: workflow_state_{self.agent_name}", flush=True)
        print(f"   Using DaprClient with token: ...{self.credentials['token'][-10:]}", flush=True)
        
        self.agent = HardcodedDurableAgent(
            name=self.config["name"],
            role=self.config["role"],
            goal=self.config["goal"],
            instructions=self.config["instructions"],
            message_bus_name="messagepubsub",
            broadcast_topic_name="beacon_channel", 
            state_store_name="workflowstatestore",
            state_key=f"workflow_state_{self.agent_name}",
            agents_registry_store_name="agentstatestore",
            agents_registry_key="agents_registry",
            dapr_client=agent_dapr_client
        )
        
        print(f"\n✅ SUCCESSFULLY CREATED {self.config['name']} ({self.agent_name})", flush=True)
        print(f"   With hardcoded token: ...{self.credentials['token'][-10:]}", flush=True)
        print(f"   Agent object: {self.agent}", flush=True)
        print(f"{'='*60}\n", flush=True)
        return self.agent
    
    async def start(self):
        """Start the agent - credentials are already hardcoded"""
        if not self.agent:
            raise RuntimeError(f"Agent {self.agent_name} not created yet")
        
        print(f"\n📡 STARTING AGENT: {self.agent.name} ({self.agent_name})", flush=True)
        print(f"   Using hardcoded token: ...{self.credentials['token'][-10:]}", flush=True)
        print(f"   Using endpoint: {self.credentials['grpc_endpoint']}", flush=True)
        print(f"   WorkflowRuntime: {getattr(self.agent, 'wf_runtime', 'Not set')}", flush=True)
        print(f"   WorkflowClient: {getattr(self.agent, 'wf_client', 'Not set')}", flush=True)
        
        # Start the agent and keep it running
        # Don't use create_task - await it properly
        self.agent_task = asyncio.create_task(self._run_agent())
        
        # Give it a moment to initialize
        await asyncio.sleep(0.5)
    
    async def _run_agent(self):
        """Run the agent continuously"""
        try:
            print(f"   🚀 Calling agent.start() for {self.agent.name}...", flush=True)
            await self.agent.start()
        except Exception as e:
            print(f"\n❌ Agent {self.agent_name} crashed: {e}", flush=True)
            import traceback
            traceback.print_exc()
            sys.stdout.flush()
            sys.stderr.flush()


def setup_agent_credentials():
    """Setup agent credentials from environment variables"""
    print("🔧 Setting up agent credentials...", flush=True)
    sys.stdout.flush()
    
    # DEBUG: Show ALL environment variables available to this process
    print("🔍 DEBUG - All DAPR environment variables in this process:", flush=True)
    dapr_vars_found = False
    for key, value in os.environ.items():
        if 'DAPR' in key:
            dapr_vars_found = True
            if 'TOKEN' in key:
                print(f"   {key}: {value[:30]}...{value[-10:] if len(value) > 40 else value}", flush=True)
            else:
                print(f"   {key}: {value}", flush=True)
    
    if not dapr_vars_found:
        print("   ⚠️  NO DAPR VARIABLES FOUND IN ENVIRONMENT!", flush=True)
    
    # Hardcoded agent list - FIXED SEQUENCE: elf -> hobbit -> wizard
    agent_names = ['elfagent', 'hobbitagent', 'wizardagent']
    print(f"📋 Using hardcoded agent list (in sequence): {agent_names}")
    print("   Order: Elf (Legolas) → Hobbit (Frodo) → Wizard (Gandalf)")
    
    valid_agents = []
    
    for agent_name in agent_names:
        print(f"\n🔍 Looking for credentials for {agent_name}...")
        
        # Get agent-specific environment variables
        prefix = agent_name.upper()
        token = os.getenv(f"DAPR_API_TOKEN_{prefix}")
        grpc_endpoint = os.getenv(f"DAPR_GRPC_ENDPOINT_{prefix}")
        http_endpoint = os.getenv(f"DAPR_HTTP_ENDPOINT_{prefix}", "")
        app_id = os.getenv(f"DAPR_APP_ID_{prefix}", agent_name)
        
        if not token:
            print(f"⚠️  No DAPR_API_TOKEN_{prefix} found for {agent_name}, skipping...")
            continue
            
        if not grpc_endpoint:
            print(f"⚠️  No DAPR_GRPC_ENDPOINT_{prefix} found for {agent_name}, skipping...")
            continue
        
        # Store credentials globally for later use
        AGENT_CREDENTIALS[agent_name] = {
            'token': token,
            'grpc_endpoint': grpc_endpoint,
            'http_endpoint': http_endpoint,
            'app_id': app_id
        }
        valid_agents.append(agent_name)
        
        print(f"✅ Stored hardcoded credentials for {agent_name}")
        print(f"   - Token ending: ...{token[-10:]}")
        print(f"   - Endpoint: {grpc_endpoint}")
        print(f"   - App ID: {app_id}")
    
    print(f"\n📊 Successfully stored credentials for {len(valid_agents)} agents: {valid_agents}", flush=True)
    sys.stdout.flush()
    return valid_agents


async def main():
    """Main function to run all agents in a single process with hardcoded credentials"""
    print("🌉 Multi-Agent Single Process Main() Starting...", flush=True)
    print("=" * 60, flush=True)
    print("🎯 All agents will run in the same process with hardcoded credentials", flush=True)
    print("🔒 NO ENVIRONMENT VARIABLE SWAPPING - credentials are hardcoded per agent", flush=True)
    print("=" * 60, flush=True)
    sys.stdout.flush()
    sys.stderr.flush()
    
    # Setup agent credentials from environment variables
    print("\n📝 Calling setup_agent_credentials()...", flush=True)
    valid_agent_names = setup_agent_credentials()
    print(f"✅ Got {len(valid_agent_names)} valid agents: {valid_agent_names}", flush=True)
    
    if not valid_agent_names:
        print("❌ No valid agents found. Check your environment variables.")
        print("   Expected format: DAPR_API_TOKEN_<AGENT>, DAPR_GRPC_ENDPOINT_<AGENT>")
        return
    
    # PHASE 1: Create all agents first (don't start them yet)
    agent_wrappers = []
    print("\n🔨 PHASE 1: Creating all agents...")
    print("=" * 60, flush=True)
    for idx, agent_name in enumerate(valid_agent_names, 1):
        if agent_name not in AGENT_CONFIGS:
            print(f"⚠️  No configuration for {agent_name}, skipping...")
            continue
        
        print(f"\n[{idx}/{len(valid_agent_names)}] Creating {agent_name}...", flush=True)
        config = AGENT_CONFIGS[agent_name]
        credentials = AGENT_CREDENTIALS[agent_name]
        print(f"   Config: {config['name']} as {config['role']}", flush=True)
        print(f"   Token: ...{credentials['token'][-10:]}", flush=True)
        
        # Create wrapper with hardcoded credentials
        wrapper = HardcodedCredentialsAgent(agent_name, config, credentials)
        agent = await wrapper.create()
        
        if agent:
            agent_wrappers.append(wrapper)
            print(f"   ✅ Agent {idx} created: {config['name']} ({agent_name})")
        
        # Small delay between agent creations for safety
        await asyncio.sleep(0.1)
    
    print(f"\n✅ PHASE 1 Complete: Created {len(agent_wrappers)} agents")
    print("=" * 60, flush=True)
    
    if not agent_wrappers:
        print("❌ No agents created successfully")
        return
    
    # PHASE 2: Start all agents (they need to be listening before orchestrator sends messages)
    print(f"\n🚀 PHASE 2: Starting all {len(agent_wrappers)} agents...")
    print("=" * 60, flush=True)
    
    # Start all agents so they're ready to receive messages
    try:
        print("\n🎯 Starting agents to listen for messages...")
        print("   (All agents must be running before workflow orchestrator sends messages)\n")
        
        for idx, wrapper in enumerate(agent_wrappers, 1):
            print(f"[{idx}/{len(agent_wrappers)}] Starting {wrapper.config['name']} ({wrapper.agent_name})...")
            
            # Start agent (it will listen for messages in background)
            await wrapper.start()
            
            # Give agent time to fully initialize and register
            await asyncio.sleep(1.0)  # Ensure proper registration
            print(f"   ✅ {wrapper.config['name']} is now listening for messages\n")
        
        print("🎉 PHASE 2 Complete: All agents are running and listening!")
        print("=" * 60, flush=True)
        
        print("\n🎆 SYSTEM READY")
        print("   - All 3 agents are running in the same process")
        print("   - Each has its own WorkflowRuntime with isolated credentials")
        print("   - Agents are listening for messages from the orchestrator")
        print("   - Workflow orchestrator can now trigger agents in sequence")
        
        # Keep all agent tasks running
        print("\n💤 System is running. Press Ctrl+C to shutdown...")
        agent_tasks = [wrapper.agent_task for wrapper in agent_wrappers if hasattr(wrapper, 'agent_task')]
        
        try:
            # Wait for all agent tasks (they should run forever)
            await asyncio.gather(*agent_tasks)
        except asyncio.CancelledError:
            print("\n🚫 Agent tasks cancelled")
        
    except KeyboardInterrupt:
        print("\n👋 Shutting down all agents...")
        
    except Exception as e:
        print(f"❌ Error running agents: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # Load environment variables
    load_dotenv()
    
    # Configure logging - ensure we see all output
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        force=True  # Override any existing configuration
    )
    
    # Ensure output is flushed immediately
    import sys
    sys.stdout.flush()
    sys.stderr.flush()
    
    print("🎯 Multi-Agent Single Process Mode with HARDCODED Credentials")
    print("🔒 Each agent uses hardcoded tokens - NO environment variable swapping")
    print(f"Process PID: {os.getpid()}")
    print("-" * 60)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Single process multi-agent system shutting down...")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
