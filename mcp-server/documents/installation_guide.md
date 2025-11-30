# Complete UV and MCP Server Setup Guide

This guide walks you through setting up UV package manager and configuring the MCP server.

## 1. clone the repo
https://github.com/Accelerator321/Cardano-MCP
## Installing UV Package Manager
Make sure to Add .env file to the cardano-mcp folder

## 2. Installing UV Package Manager

### Windows
```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### macOS/Linux
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## 3- Setting Up Virtual Environment and Installing Requirements

a. Open terminal in your Cardano-MCP folder



b. Add MCP CLI and httpx dependencies:
```bash
uv add mcp[cli] httpx
```

c. Install required packages:
```bash
uv sync
```




## 3 MCP Server Configuration

open  `cloud_desktop_config.json` file with the following content:

```json
{
   "mcpServers": {
       "cardano_mcp": {
           "command": "uv",
           "args": [
               "--directory",
               "D:\\Cardano\\Cardano-MCP",
               "run",
               "server.py"
           ],
           "env": {
               "TRANSPORT": "stdio"
           }
       }
   }
}
```
now change "/ABSOLUTE/PATH/TO/PARENT/FOLDER/Cardano-MCP" to "your_cardano-mcp-path(where server file is present)" and save the changes. make sure if you path has single backward slashes(\\) - "c:\\home\\user" you convert it to "c:\\\\home\\\\user" . If your path has forward slashes(/) you can paste it as it is.


**Important Notes:**
- Replace `/ABSOLUTE/PATH/TO/PARENT/FOLDER/Cardano-MCP` with the actual absolute path to your Cardano-MCP installation directory.
- Examples:
  * Windows: `C:\\Users\\username\\Cardano-MCP`
  * Linux: `/home/username/Cardano-MCP`
  * macOS: `/Users/username/Cardano-MCP`
- The configuration directly uses UV to run your server script without requiring separate activation steps.
- The server file should be named `server.py` and located in your MCP installation directory.

## Usage Notes
1. Ensure UV is properly installed and available in your PATH
2. The configuration uses UV to run the server script directly
3. Always use absolute paths in the configuration file
4. Make sure your server.py file is in the specified directory