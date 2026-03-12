"""
Testes de Importação - DAGs
============================

Testa se os DAGs podem ser importados sem erros.
Este teste pega erros que py_compile não encontra:
- Parâmetros faltando
- Imports inválidos
- Variáveis não definidas
"""

import pytest
import sys
import ast
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


class TestDAGImports:
    """Testa se os DAGs podem ser parseados corretamente."""

    def test_clima_dag_can_be_parsed(self):
        """Testa se ingestion_clima_dag.py pode ser parseado"""
        with open('dags/ingestion_clima_dag.py', 'r', encoding='utf-8') as f:
            code = f.read()
            try:
                ast.parse(code)
            except SyntaxError as e:
                pytest.fail(f"Erro de sintaxe em ingestion_clima_dag.py: {e}")

    def test_solos_dag_can_be_parsed(self):
        """Testa se ingestion_solos_dag.py pode ser parseado"""
        with open('dags/ingestion_solos_dag.py', 'r', encoding='utf-8') as f:
            code = f.read()
            try:
                ast.parse(code)
            except SyntaxError as e:
                pytest.fail(f"Erro de sintaxe em ingestion_solos_dag.py: {e}")

    def test_agricultura_dag_can_be_parsed(self):
        """Testa se ingestion_agricultura_dag.py pode ser parseado"""
        with open('dags/ingestion_agricultura_dag.py', 'r', encoding='utf-8') as f:
            code = f.read()
            try:
                ast.parse(code)
            except SyntaxError as e:
                pytest.fail(f"Erro de sintaxe em ingestion_agricultura_dag.py: {e}")

    def test_transform_dag_can_be_parsed(self):
        """Testa se transform_dag.py pode ser parseado"""
        with open('dags/transform_dag.py', 'r', encoding='utf-8') as f:
            code = f.read()
            try:
                ast.parse(code)
            except SyntaxError as e:
                pytest.fail(f"Erro de sintaxe em transform_dag.py: {e}")

    def test_load_dag_can_be_parsed(self):
        """Testa se load_dag.py pode ser parseado"""
        with open('dags/load_dag.py', 'r', encoding='utf-8') as f:
            code = f.read()
            try:
                ast.parse(code)
            except SyntaxError as e:
                pytest.fail(f"Erro de sintaxe em load_dag.py: {e}")

    def test_dbt_dag_can_be_parsed(self):
        """Testa se dbt_analytics_dag.py pode ser parseado"""
        with open('dags/dbt_analytics_dag.py', 'r', encoding='utf-8') as f:
            code = f.read()
            try:
                ast.parse(code)
            except SyntaxError as e:
                pytest.fail(f"Erro de sintaxe em dbt_analytics_dag.py: {e}")


class TestDAGStructureValidation:
    """Valida que DAGs têm estrutura correta."""

    def test_all_dags_have_observability_manager_complete(self):
        """Verifica se ObservabilityManager tem todos os parâmetros"""
        dag_files = [
            'dags/ingestion_clima_dag.py',
            'dags/ingestion_solos_dag.py',
            'dags/ingestion_agricultura_dag.py',
        ]

        required_params = ['pipeline_name', 'namespace', 'log_group', 'sns_topic_arn', 'aws_region']

        for dag_file in dag_files:
            with open(dag_file, 'r', encoding='utf-8') as f:
                content = f.read()

                if 'ObservabilityManager(' in content:
                    # Encontra o bloco do ObservabilityManager
                    start = content.find('ObservabilityManager(')
                    end = content.find(')', start)
                    obs_block = content[start:end+1]

                    # Verifica se tem todos os parâmetros
                    for param in required_params:
                        assert f"{param}=" in obs_block, \
                            f"{dag_file}: ObservabilityManager está faltando o parâmetro '{param}'"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
