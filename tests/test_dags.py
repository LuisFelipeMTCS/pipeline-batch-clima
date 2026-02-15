"""
Testes Unitários - DAGs
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


class TestClimaDAG:
    
    def test_importa(self):
        from dags.clima_dag import dag
        assert dag is not None
    
    def test_id_correto(self):
        from dags.clima_dag import dag
        assert dag.dag_id == "pipeline_clima"
    
    def test_tem_tasks(self):
        from dags.clima_dag import dag
        assert len(dag.tasks) > 0


class TestSolosDAG:
    
    def test_importa(self):
        from dags.solos_dag import dag
        assert dag is not None
    
    def test_id_correto(self):
        from dags.solos_dag import dag
        assert dag.dag_id == "pipeline_solos"


class TestAgriculturaDAG:
    
    def test_importa(self):
        from dags.agricultura_dag import dag
        assert dag is not None
    
    def test_id_correto(self):
        from dags.agricultura_dag import dag
        assert dag.dag_id == "pipeline_agricultura"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
