import yaml

from pathlib import Path
from prefect.blocks.core import Block, register_block
from typing import Dict, Optional

@register_block
class KubernetesCluster(Block):
    context: str
    config_file: Optional[str] = f"{Path.home()}/.kube/config"

    @property
    def config(self) -> Dict: 
        with open(self.config_file, 'r') as f:
            config_contents = yaml.safe_load(f)
            for cluster_config in config_contents['clusters']:
                if cluster_config['name'] == self.context:
                    return cluster_config['cluster']
