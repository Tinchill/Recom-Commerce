import torch
import torch.nn as nn 
import torch.nn.functional as F 
from torch.utils.tensorboard import SummaryWriter

class LocalActivationUnit(nn.Module):
    def __init__(self, hidden_units):
        super(LocalActivationUnit, self).__init__()
        self.fc1 = nn.Linear(hidden_units * 4, hidden_units)
        self.fc2 = nn.Linear(hidden_units, 1)

    def forward(self, user_behaviors, target_item, mask):
        # 用户行为tensor各维度: (batch_size, seq_len, hidden_units)
        # 目标商品tensor各维度: (batch_size, hidden_units)
        # mask 各维度: (batch_size, seq_len)
        
        seq_len = user_behaviors.size(1)
        target_item = target_item.unsqueeze(1).expand(-1, seq_len, -1)
        
        # 将用户行为 embeddings 与目标商品 embeddings 进行拼接
        interactions = torch.cat([user_behaviors, target_item, user_behaviors-target_item, user_behaviors*target_item], dim=-1)
       
        x = torch.relu(self.fc1(interactions))
        attention_logits = self.fc2(x).squeeze(-1)
        
        # 应用 mask 消除 padding 影响
        attention_logits = attention_logits.masked_fill(mask == 0, float('-inf'))
        
        # 计算 attention 权重
        attention_weights = F.softmax(attention_logits, dim=1).unsqueeze(-1)
        
        # 计算用户行为 embeddings 的加权和以得到用户兴趣
        user_interests = torch.sum(attention_weights * user_behaviors, dim=1)
        return user_interests
    
class Expert(nn.Module):
    def __init__(self, input_size):
        super(Expert, self).__init__()
        self.network = nn.Sequential(
            nn.Linear(input_size, 512),
            nn.ReLU(),
            nn.Linear(512, 128),
            nn.ReLU(),
            nn.Linear(128, 1)
        )
        
    def forward(self, x):
        return self.network(x)
    
class Gate(nn.Module):
    def __init__(self, input_size, num_experts):
        super(Gate, self).__init__()
        self.network = nn.Sequential(
            nn.Linear(input_size, 64),
            nn.ReLU(),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, num_experts)
        )
        
    def forward(self, x):
        return self.network(x)
    
class MoeModel(nn.Module):
    def __init__(self, config):
        super(MoeModel, self).__init__()
        self.config = config
        self.embedding = nn.Embedding(num_embeddings=self.config["num_embedding"], embedding_dim =self.config["embedding_dim"], padding_idx=0)
        
        self.att = LocalActivationUnit(self.config["embedding_dim"])
        self.experts = nn.ModuleList([Expert(self.config["embedding_dim"]*len(self.config["feature_col"])) for _ in range(self.config["num_experts"])])
        self.gate = Gate(self.config["embedding_dim"]*len(self.config["features_gate_col"]), self.config["num_experts"])
        
    def forward(self, features, mask):
        embedding_dict = {}
        for ff in self.config["feature_col"]:
            if ff != 'pay_brand_seq' :
                embedding_dict[ff] = torch.sum(self.embedding(features[ff]), dim=1)
        embedding_dict['pay_brand_seq'] = self.att(self.embedding(features['pay_brand_seq']), embedding_dict['target_brand_id'], mask['pay_brand_seq'])
        x = torch.cat([embedding_dict[ff] for ff in self.config["feature_col"]], dim=1)
        gate_emb = torch.cat([embedding_dict[ff] for ff in self.config["features_gate_col"]], dim=1)
        gating_weights = F.softmax(self.gate(gate_emb), dim=1)
        
        # 通过DNN
        expert_outputs = torch.stack([expert(x) for expert in self.experts], dim=-1)
        x = torch.sum(gating_weights* expert_outputs.squeeze(), dim=-1)

        return x,gating_weights
