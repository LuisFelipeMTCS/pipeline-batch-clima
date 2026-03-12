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

    def test_ingestion_clima_dag_syntax(self):
        """Verifica sintaxe do ingestion_clima_dag.py"""
        import py_compile
        py_compile.compile('dags/ingestion_clima_dag.py', doraise=True)

    def test_ingestion_solos_dag_syntax(self):
        """Verifica sintaxe do ingestion_solos_dag.py"""
        import py_compile
        py_compile.compile('dags/ingestion_solos_dag.py', doraise=True)

    def test_ingestion_agricultura_dag_syntax(self):
        """Verifica sintaxe do ingestion_agricultura_dag.py"""
        import py_compile
        py_compile.compile('dags/ingestion_agricultura_dag.py', doraise=True)

    def test_transform_dag_syntax(self):
        """Verifica sintaxe do transform_dag.py"""
        import py_compile
        py_compile.compile('dags/transform_dag.py', doraise=True)

    def test_load_dag_syntax(self):
        """Verifica sintaxe do load_dag.py"""
        import py_compile
        py_compile.compile('dags/load_dag.py', doraise=True)

    def test_dbt_analytics_dag_syntax(self):
        """Verifica sintaxe do dbt_analytics_dag.py"""
        import py_compile
        py_compile.compile('dags/dbt_analytics_dag.py', doraise=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])