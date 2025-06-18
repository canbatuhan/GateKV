import paramiko

USERNAME = "pi_user"
PASSWORD = "tolga.halit.batu"

PATH = "/home/pi_user/GateKV/"

NODES = {
    "gateway-00": {"host": "192.168.1.120",
                   "file": "gateway.py",
                   "config": "docs/gateway-00.yaml"},

    "gateway-01": {"host": "192.168.1.129",
                   "file": "gateway.py",
                   "config": "docs/gateway-01.yaml"},

    "storage-00": {"host": "192.168.1.121",
                   "file": "storage.py",
                   "config": "docs/storage-00.yaml"},

    "storage-01": {"host": "192.168.1.123",
                   "file": "storage.py",
                   "config": "docs/storage-01.yaml"},

    "storage-02": {"host": "192.168.1.125",
                   "file": "storage.py",
                   "config": "docs/storage-02.yaml"},

    "storage-03": {"host": "192.168.1.127",
                   "file": "storage.py",
                   "config": "docs/storage-03.yaml"},

    "storage-04": {"host": "192.168.1.128",
                   "file": "storage.py",
                   "config": "docs/storage-04.yaml"},
}

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

for node, config in NODES.items():
    remote_cmd = f"nohup python {PATH+config.get('file')} --config={PATH+config.get('config')} &"

    try:
        print(f"Connecting to {config.get('host')}...")
        client.connect(hostname=config.get("host"), username=USERNAME, password=PASSWORD)

        print("Executing remote command...")
        stdin, stdout, stderr = client.exec_command(remote_cmd)

        error_output = stderr.read().decode()
        if error_output:
            print("Error:", error_output)
        else:
            print("Script started successfully in background.")

    finally:
        client.close()
        print("Disconnected from host.")
