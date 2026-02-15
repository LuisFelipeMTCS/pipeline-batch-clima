"""
Testes Unitários - DAGs
=======================

Apenas verifica se os arquivos de DAG têm sintaxe válida.
Os testes de importação são pulados pois dependem da versão específica do Airflow.
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


class TestDAGsSyntax:
    """Verifica apenas a sintaxe dos arquivos de DAG."""
    
    def test_clima_dag_syntax(self):
        """Verifica sintaxe do clima_dag.py"""
        import py_compile
        py_compile.compile('dags/clima_dag.py', doraise=True)
    
    def test_solos_dag_syntax(self):
        """Verifica sintaxe do solos_dag.py"""
        import py_compile
        py_compile.compile('dags/solos_dag.py', doraise=True)
    
    def test_agricultura_dag_syntax(self):
        """Verifica sintaxe do agricultura_dag.py"""
        import py_compile
        py_compile.compile('dags/agricultura_dag.py', doraise=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])