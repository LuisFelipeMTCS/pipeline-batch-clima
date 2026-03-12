"""
Testes Unitários - DAGs
=======================

Verifica sintaxe e importação dos DAGs.
"""

import pytest
import py_compile
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


class TestDAGsSyntax:
    """Verifica sintaxe dos arquivos de DAG."""

    def test_ingestion_clima_dag_syntax(self):
        """Verifica sintaxe do ingestion_clima_dag.py"""
        py_compile.compile('dags/ingestion_clima_dag.py', doraise=True)

    def test_ingestion_solos_dag_syntax(self):
        """Verifica sintaxe do ingestion_solos_dag.py"""
        py_compile.compile('dags/ingestion_solos_dag.py', doraise=True)

    def test_ingestion_agricultura_dag_syntax(self):
        """Verifica sintaxe do ingestion_agricultura_dag.py"""
        py_compile.compile('dags/ingestion_agricultura_dag.py', doraise=True)

    def test_transform_dag_syntax(self):
        """Verifica sintaxe do transform_dag.py"""
        py_compile.compile('dags/transform_dag.py', doraise=True)

    def test_load_dag_syntax(self):
        """Verifica sintaxe do load_dag.py"""
        py_compile.compile('dags/load_dag.py', doraise=True)

    def test_dbt_analytics_dag_syntax(self):
        """Verifica sintaxe do dbt_analytics_dag.py"""
        py_compile.compile('dags/dbt_analytics_dag.py', doraise=True)


class TestDAGsStructure:
    """Verifica estrutura básica dos DAGs (sem Airflow)."""

    def test_all_dags_have_valid_python(self):
        """Testa se todos os DAGs têm Python válido"""
        dag_files = [
            'dags/ingestion_clima_dag.py',
            'dags/ingestion_solos_dag.py',
            'dags/ingestion_agricultura_dag.py',
            'dags/transform_dag.py',
            'dags/load_dag.py',
            'dags/dbt_analytics_dag.py',
        ]

        for dag_file in dag_files:
            # Compila para verificar sintaxe
            py_compile.compile(dag_file, doraise=True)

            # Lê o arquivo e verifica estrutura básica
            with open(dag_file, 'r', encoding='utf-8') as f:
                content = f.read()

                # Verifica imports básicos esperados
                assert 'from airflow' in content, f"{dag_file}: Deve importar airflow"
                assert 'DAG' in content or 'dag' in content, f"{dag_file}: Deve definir uma DAG"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])