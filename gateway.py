import yaml
import argparse

from gatekv.gateway import GateKV_GatewayNode_Runner

parser = argparse.ArgumentParser()
parser.add_argument("--config")
args = vars(parser.parse_args())

CONFIG = yaml.safe_load(open(args["config"]))

if __name__ == "__main__":
    gateway_node = GateKV_GatewayNode_Runner(CONFIG)
    gateway_node.run()