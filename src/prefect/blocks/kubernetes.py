from kubernetes import config
from kubernetes.client import Configuration

from prefect.blocks.core import Block, register_block

@register_block
class KubernetesCluster(Block):
    config_file: str
    context: str
    
#     client_configuration: 
#     persist_config: bool = False

# @register_block
# class KubernetesClientConfig(Block):