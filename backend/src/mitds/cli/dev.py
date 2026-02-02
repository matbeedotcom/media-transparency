"""CLI commands for development utilities.

Provides command-line tools for local development:
- Cloudflare Tunnel for HTTPS OAuth callbacks
- Environment configuration helpers

Usage:
    mitds dev tunnel [--port PORT]
    mitds dev tunnel --stop
"""

import asyncio
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

import click


@click.group(name="dev")
def cli():
    """Development utility commands."""
    pass


# Global to track the tunnel process
_tunnel_process = None


def _find_cloudflared() -> str | None:
    """Find cloudflared executable."""
    # Check if cloudflared is in PATH
    import shutil
    cloudflared = shutil.which("cloudflared")
    if cloudflared:
        return cloudflared
    
    # Check common locations
    common_paths = [
        "/usr/local/bin/cloudflared",
        "/usr/bin/cloudflared",
        os.path.expanduser("~/.cloudflared/cloudflared"),
        os.path.expanduser("~/bin/cloudflared"),
        # Windows paths
        os.path.expanduser("~\\cloudflared\\cloudflared.exe"),
        "C:\\Program Files\\cloudflared\\cloudflared.exe",
        "C:\\Program Files (x86)\\cloudflared\\cloudflared.exe",
    ]
    
    for path in common_paths:
        if os.path.isfile(path):
            return path
    
    return None


def _update_env_file(key: str, value: str, env_path: Path) -> bool:
    """Update or add a key in the .env file."""
    if not env_path.exists():
        click.echo(f"Warning: {env_path} not found, creating it")
        env_path.write_text(f"{key}={value}\n")
        return True
    
    content = env_path.read_text()
    lines = content.splitlines()
    
    # Find and replace the key, or add it
    found = False
    new_lines = []
    for line in lines:
        if line.startswith(f"{key}=") or line.startswith(f"# {key}="):
            new_lines.append(f"{key}={value}")
            found = True
        else:
            new_lines.append(line)
    
    if not found:
        new_lines.append(f"{key}={value}")
    
    env_path.write_text("\n".join(new_lines) + "\n")
    return True


@cli.command(name="tunnel")
@click.option(
    "--port",
    type=int,
    default=8000,
    help="Local port to tunnel (default: 8000)",
)
@click.option(
    "--update-env/--no-update-env",
    default=True,
    help="Automatically update .env with the tunnel URL (default: yes)",
)
@click.option(
    "--stop",
    is_flag=True,
    help="Stop any running tunnel",
)
def tunnel_command(port: int, update_env: bool, stop: bool):
    """Start a Cloudflare Tunnel for HTTPS OAuth callbacks.

    This creates a temporary public HTTPS URL that tunnels to your local
    development server, allowing Meta OAuth callbacks to work without
    setting up SSL certificates.

    The tunnel URL will be automatically configured as META_OAUTH_REDIRECT_URI.

    Requirements:
        - Install cloudflared: https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/

    Examples:
        mitds dev tunnel              # Start tunnel on port 8000
        mitds dev tunnel --port 8001  # Start tunnel on custom port
        mitds dev tunnel --stop       # Stop the tunnel
    """
    global _tunnel_process
    
    if stop:
        click.echo("Stopping any running cloudflared tunnels...")
        if sys.platform == "win32":
            os.system("taskkill /f /im cloudflared.exe 2>nul")
        else:
            os.system("pkill -f 'cloudflared tunnel' 2>/dev/null")
        click.echo("Done.")
        return
    
    # Find cloudflared
    cloudflared = _find_cloudflared()
    if not cloudflared:
        click.echo("Error: cloudflared not found!", err=True)
        click.echo("")
        click.echo("Please install cloudflared:")
        click.echo("  - macOS: brew install cloudflared")
        click.echo("  - Windows: winget install Cloudflare.cloudflared")
        click.echo("  - Linux: See https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/")
        sys.exit(1)
    
    click.echo(f"Found cloudflared at: {cloudflared}")
    click.echo(f"Starting tunnel to localhost:{port}...")
    click.echo("")
    
    # Start cloudflared tunnel
    try:
        # Use quick tunnel (no account required)
        process = subprocess.Popen(
            [cloudflared, "tunnel", "--url", f"http://localhost:{port}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        _tunnel_process = process
        
        # Wait for the URL to appear in output
        tunnel_url = None
        start_time = time.time()
        timeout = 30  # seconds
        
        click.echo("Waiting for tunnel URL...")
        
        while time.time() - start_time < timeout:
            line = process.stdout.readline()
            if not line:
                if process.poll() is not None:
                    click.echo("Error: cloudflared exited unexpectedly", err=True)
                    sys.exit(1)
                continue
            
            # Look for the tunnel URL in output
            # Format: "https://random-words.trycloudflare.com"
            match = re.search(r'(https://[a-z0-9-]+\.trycloudflare\.com)', line)
            if match:
                tunnel_url = match.group(1)
                break
            
            # Also check for errors
            if "error" in line.lower() and "failed" in line.lower():
                click.echo(f"Error: {line.strip()}", err=True)
        
        if not tunnel_url:
            click.echo("Error: Could not get tunnel URL within timeout", err=True)
            process.terminate()
            sys.exit(1)
        
        # Build the OAuth callback URL
        callback_url = f"{tunnel_url}/api/v1/meta/auth/callback"
        
        click.echo("=" * 60)
        click.echo(f"Tunnel URL: {tunnel_url}")
        click.echo(f"OAuth Callback URL: {callback_url}")
        click.echo("=" * 60)
        click.echo("")
        
        # Update .env file
        if update_env:
            # The backend reads from backend/.env when running from backend/ directory
            # So we need to update that file, not the root .env
            backend_env = Path(__file__).resolve().parent.parent.parent.parent / ".env"
            root_env = backend_env.parent / ".env"
            
            # Prefer backend/.env if it exists, otherwise root .env
            if backend_env.exists():
                env_path = backend_env
            elif root_env.exists():
                env_path = root_env
            else:
                env_path = backend_env  # Create in backend/
            
            if env_path.exists() or update_env:
                click.echo(f"Updating {env_path}...")
                _update_env_file("META_OAUTH_REDIRECT_URI", callback_url, env_path)
                click.echo("  META_OAUTH_REDIRECT_URI updated")
                
                # Also update frontend redirect to use tunnel
                frontend_url = f"{tunnel_url}/settings"
                # Don't update frontend redirect - it should stay as localhost for the browser
                # _update_env_file("META_OAUTH_FRONTEND_REDIRECT", frontend_url, env_path)
                
                click.echo("")
        
        click.echo("IMPORTANT: Add this URL to your Meta App's Valid OAuth Redirect URIs:")
        click.echo(f"  {callback_url}")
        click.echo("")
        click.echo("Meta Developer Console:")
        click.echo("  https://developers.facebook.com/apps/ > Your App > Facebook Login > Settings")
        click.echo("")
        click.echo("Press Ctrl+C to stop the tunnel...")
        click.echo("")
        
        # Keep running and show output
        try:
            while True:
                line = process.stdout.readline()
                if not line:
                    if process.poll() is not None:
                        break
                    continue
                # Only show important lines
                if any(x in line.lower() for x in ["error", "warn", "connected", "disconnected"]):
                    click.echo(f"[cloudflared] {line.strip()}")
        except KeyboardInterrupt:
            click.echo("\nStopping tunnel...")
            process.terminate()
            process.wait(timeout=5)
            click.echo("Tunnel stopped.")
            
    except FileNotFoundError:
        click.echo(f"Error: Could not execute {cloudflared}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command(name="oauth-setup")
def oauth_setup_command():
    """Show instructions for setting up Meta OAuth.

    Displays step-by-step instructions for configuring Meta OAuth
    for local development and production.
    """
    click.echo("""
Meta OAuth Setup Instructions
=============================

1. CREATE A META APP (if you don't have one)
   - Go to: https://developers.facebook.com/apps/
   - Click "Create App" > "Other" > "Business"
   - Note your App ID and App Secret

2. CONFIGURE FACEBOOK LOGIN
   - In your app dashboard, add the "Facebook Login" product
   - Go to Facebook Login > Settings
   - Add your OAuth Redirect URI:
     
     For LOCAL DEVELOPMENT (with cloudflared tunnel):
       Run: mitds dev tunnel
       Add the generated URL (e.g., https://xxx.trycloudflare.com/api/v1/meta/auth/callback)
     
     For PRODUCTION:
       https://your-domain.com/api/v1/meta/auth/callback

3. SET ENVIRONMENT VARIABLES
   In your .env file:
   
   META_APP_ID=your-app-id
   META_APP_SECRET=your-app-secret
   
   # For production only:
   META_OAUTH_REDIRECT_URI=https://your-domain.com/api/v1/meta/auth/callback
   META_OAUTH_FRONTEND_REDIRECT=https://your-domain.com/settings

4. REQUEST PERMISSIONS (for Ad Library API)
   - Go to App Review > Permissions and Features
   - Request "ads_read" permission
   - For full page details, also request "pages_read_engagement"

5. TEST THE FLOW
   - Start your backend: uvicorn main:app --reload
   - Start the tunnel: mitds dev tunnel
   - Open: http://localhost:5173/settings
   - Click "Connect Facebook"

Need help? Check the Meta docs:
  - https://developers.facebook.com/docs/facebook-login/
  - https://developers.facebook.com/docs/marketing-api/overview/authorization
""")
