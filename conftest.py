"""
Configuracao do pytest para o projeto de scraping.
"""

import sys
from pathlib import Path

# Adiciona o diretorio src ao path para importacoes
src_path = Path(__file__).parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))
