import torch
from torch.utils.data import Dataset
import torch.nn as nn

dev = torch.device("cuda") if torch.cuda.is_available() else torch.device("cpu")


class SiameseNetworkDataset(Dataset):

    def __init__(self, data):
        self.data = data

    def __getitem__(self, index):
        anch = self.data[index][0]
        pos = self.data[index][1]
        neg = self.data[index][2]

        return anch, pos, neg

    def __len__(self):
        return len(self.data)


class SiameseNetwork(nn.Module):
    def __init__(self, embed_dim, hidden_dim1, hidden_dim2, output_dim):
        super(SiameseNetwork, self).__init__()
        self._lw1 = torch.nn.parameter.Parameter(torch.randn(embed_dim, hidden_dim1))  # embed_dim -> 784,512
        self._l1 = nn.Linear(embed_dim, hidden_dim1) # old: (embed_dim, 512)
        self._relu = nn.ReLU(inplace=True)
        self._l2 = nn.Linear(hidden_dim1, hidden_dim2) # old: (512, 128)
        self._l3 = nn.Linear(hidden_dim2, output_dim) # old: (128, 10)

    def forward_once(self, x):
        # if you use dense vectors use the row below instead of x.bmm and lw1
        # x = self._l1(x)
        b = x.shape[0]
        x = x.bmm(self._lw1.repeat(b, 1, 1))
        x = self._relu(x)
        x = self._l2(x)
        x = self._relu(x)
        x = self._l3(x)
        return x

    def forward(self, input1, input2, input3):
        # expects tensor of type Float
        # input1 = input1.type(torch.FloatTensor)
        # input2 = input2.type(torch.FloatTensor)
        # input3 = input3.type(torch.FloatTensor)
        output1 = self.forward_once(input1)
        output2 = self.forward_once(input2)
        output3 = self.forward_once(input3)
        return output1.to(dev), output2.to(dev), output3.to(dev)
