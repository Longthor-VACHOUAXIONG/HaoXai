# Package Installation Examples in HaoXai Notebook

## Method 1: Using the Package Installer UI
1. In the notebook, find the "Install Package" section in the sidebar
2. Enter package name (e.g., `seaborn`, `plotly`, `scikit-learn`)
3. Click "Install Package"

## Method 2: Using Python pip in a cell
```python
# Install packages directly in Python cells
import subprocess
import sys

# Install seaborn for data visualization
subprocess.check_call([sys.executable, "-m", "pip", "install", "seaborn"])

# Install plotly for interactive plots
subprocess.check_call([sys.executable, "-m", "pip", "install", "plotly"])

# Install scikit-learn for machine learning
subprocess.check_call([sys.executable, "-m", "pip", "install", "scikit-learn"])
```

## Popular Data Science Libraries to Install:
- `seaborn` - Statistical data visualization
- `plotly` - Interactive plots and charts
- `scikit-learn` - Machine learning algorithms
- `matplotlib` - Plotting and visualization
- `numpy` - Numerical computing
- `scipy` - Scientific computing
- `openpyxl` - Excel file manipulation
- `xlrd` - Reading Excel files
