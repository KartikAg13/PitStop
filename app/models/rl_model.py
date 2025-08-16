import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
import numpy as np

# -------------------------------
# 1. Dataset (replace with FastF1 later)
# -------------------------------
class LapDataset(Dataset):
    def __init__(self, num_samples=1000, seq_len=50, feature_dim=10, num_classes=4):
        super().__init__()
        # Fake lap data: (num_samples, seq_len, feature_dim)
        self.data = torch.randn(num_samples, seq_len, feature_dim)
        
        # Fake labels: 0=no pit, 1=soft, 2=medium, 3=hard
        self.labels = torch.randint(0, num_classes, (num_samples,))

    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx):
        return self.data[idx], self.labels[idx]


# -------------------------------
# 2. Transformer Model
# -------------------------------
class F1Transformer(nn.Module):
    def __init__(self, feature_dim, d_model=64, nhead=4, num_layers=2, num_classes=4, max_len=100):
        super(F1Transformer, self).__init__()

        # Project numeric lap features into embedding space
        self.input_proj = nn.Linear(feature_dim, d_model)

        # Positional encoding (trainable)
        self.pos_embedding = nn.Parameter(torch.randn(1, max_len, d_model))

        # Transformer Encoder
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=d_model, 
            nhead=nhead, 
            batch_first=True
        )
        self.transformer = nn.TransformerEncoder(encoder_layer, num_layers=num_layers)

        # Classification head
        self.fc_out = nn.Linear(d_model, num_classes)

    def forward(self, x):
        """
        x: shape (batch_size, seq_len, feature_dim)
        """
        b, seq_len, _ = x.size()

        # Project features
        x = self.input_proj(x)  # (b, seq_len, d_model)

        # Add positional embeddings
        x = x + self.pos_embedding[:, :seq_len, :]

        # Pass through Transformer
        x = self.transformer(x)  # (b, seq_len, d_model)

        # Use last lap for decision
        x = x[:, -1, :]  # (b, d_model)

        # Predict pit decision
        out = self.fc_out(x)  # (b, num_classes)

        return out


# -------------------------------
# 3. Training & Evaluation
# -------------------------------
def train_model(model, dataloader, optimizer, criterion, device):
    model.train()
    total_loss, correct, total = 0, 0, 0

    for x, y in dataloader:
        x, y = x.to(device), y.to(device)

        optimizer.zero_grad()
        outputs = model(x)

        loss = criterion(outputs, y)
        loss.backward()
        optimizer.step()

        total_loss += loss.item()
        _, predicted = torch.max(outputs, 1)
        correct += (predicted == y).sum().item()
        total += y.size(0)

    return total_loss / len(dataloader), correct / total


def eval_model(model, dataloader, criterion, device):
    model.eval()
    total_loss, correct, total = 0, 0, 0

    with torch.no_grad():
        for x, y in dataloader:
            x, y = x.to(device), y.to(device)
            outputs = model(x)

            loss = criterion(outputs, y)
            total_loss += loss.item()

            _, predicted = torch.max(outputs, 1)
            correct += (predicted == y).sum().item()
            total += y.size(0)

    return total_loss / len(dataloader), correct / total


# -------------------------------
# 4. Main script
# -------------------------------
if __name__ == "__main__":
    # Settings
    batch_size = 32
    seq_len = 50   # laps per race
    feature_dim = 10  # lap features (lap_time, tyre_age, compound, track_temp, etc.)
    num_classes = 4  # no pit, soft, medium, hard
    epochs = 5
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    # Data
    dataset = LapDataset(num_samples=1000, seq_len=seq_len, feature_dim=feature_dim, num_classes=num_classes)
    train_size = int(0.8 * len(dataset))
    test_size = len(dataset) - train_size
    train_dataset, test_dataset = torch.utils.data.random_split(dataset, [train_size, test_size])

    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
    test_loader = DataLoader(test_dataset, batch_size=batch_size)

    # Model
    model = F1Transformer(feature_dim=feature_dim, num_classes=num_classes).to(device)
    optimizer = optim.Adam(model.parameters(), lr=1e-3)
    criterion = nn.CrossEntropyLoss()

    # Training loop
    for epoch in range(epochs):
        train_loss, train_acc = train_model(model, train_loader, optimizer, criterion, device)
        test_loss, test_acc = eval_model(model, test_loader, criterion, device)

        print(f"Epoch {epoch+1}/{epochs}")
        print(f"  Train Loss: {train_loss:.4f}, Train Acc: {train_acc:.4f}")
        print(f"  Test Loss:  {test_loss:.4f}, Test Acc:  {test_acc:.4f}")
