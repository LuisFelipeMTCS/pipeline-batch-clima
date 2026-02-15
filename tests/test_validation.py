"""
Testes Unitários - Validação
"""

import pytest
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.validation.engine import (
    ValidationEngine,
    ValidationConfig,
    NotEmptyRule,
    RequiredColumnsRule,
    NotNullRule,
    NumericRangeRule,
)


class TestNotEmptyRule:
    
    def test_dataframe_vazio_deve_falhar(self):
        df = pd.DataFrame()
        rule = NotEmptyRule()
        errors = rule.validate(df)
        assert len(errors) == 1
    
    def test_dataframe_com_dados_deve_passar(self):
        df = pd.DataFrame({"col1": [1, 2, 3]})
        rule = NotEmptyRule()
        errors = rule.validate(df)
        assert len(errors) == 0


class TestRequiredColumnsRule:
    
    def test_colunas_faltando_deve_falhar(self):
        df = pd.DataFrame({"col1": [1]})
        rule = RequiredColumnsRule(["col1", "col2"])
        errors = rule.validate(df)
        assert len(errors) == 1
    
    def test_todas_colunas_presentes_deve_passar(self):
        df = pd.DataFrame({"col1": [1], "col2": [2]})
        rule = RequiredColumnsRule(["col1", "col2"])
        errors = rule.validate(df)
        assert len(errors) == 0


class TestNotNullRule:
    
    def test_valores_nulos_deve_falhar(self):
        df = pd.DataFrame({"col1": [1, None, 3]})
        rule = NotNullRule(["col1"])
        errors = rule.validate(df)
        assert len(errors) == 1
    
    def test_sem_nulos_deve_passar(self):
        df = pd.DataFrame({"col1": [1, 2, 3]})
        rule = NotNullRule(["col1"])
        errors = rule.validate(df)
        assert len(errors) == 0


class TestNumericRangeRule:
    
    def test_valores_fora_range_deve_falhar(self):
        df = pd.DataFrame({"temp": [-50, 10, 100]})
        rule = NumericRangeRule("temp", min_value=-15, max_value=50)
        errors = rule.validate(df)
        assert len(errors) >= 1
    
    def test_valores_no_range_deve_passar(self):
        df = pd.DataFrame({"temp": [10, 20, 30]})
        rule = NumericRangeRule("temp", min_value=-15, max_value=50)
        errors = rule.validate(df)
        assert len(errors) == 0


class TestValidationEngine:
    
    def test_engine_executa_regras(self):
        df = pd.DataFrame({"col1": [1, 2]})
        config = ValidationConfig(pipeline_name="Teste", fail_on_error=False)
        engine = ValidationEngine(config)
        rules = [NotEmptyRule()]
        result = engine.run(df, rules)
        assert result.is_valid == True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
