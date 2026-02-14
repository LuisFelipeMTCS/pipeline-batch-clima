"""
Validação - AGRICULTURA
=======================

Regras específicas para dados de agricultura.
"""

import pandas as pd
from typing import List

from src.validation.engine import (
    ValidationEngine,
    ValidationConfig,
    ValidationResult,
    ValidationRule,
    ValidationError,
    NotEmptyRule,
    RequiredColumnsRule,
    NotNullRule,
    NumericRangeRule,
)


# =========================
# REGRAS ESPECÍFICAS AGRICULTURA
# =========================

class AreaValidaRule(ValidationRule):
    """Valida que área está em range válido (> 0)."""
    
    @property
    def name(self) -> str:
        return "area_valida"
    
    def validate(self, df: pd.DataFrame) -> List[ValidationError]:
        errors = []
        
        area_cols = [col for col in df.columns if 'AREA' in col.upper() or 'HECTARE' in col.upper()]
        
        for col in area_cols:
            data = pd.to_numeric(df[col], errors='coerce')
            
            negativo = (data < 0) & data.notna()
            count = negativo.sum()
            
            if count > 0:
                errors.append(ValidationError(
                    rule=self.name,
                    column=col,
                    message=f"Coluna '{col}' tem {count} valores negativos",
                    rows_affected=count
                ))
        
        return errors


class ProducaoValidaRule(ValidationRule):
    """Valida que produção/rendimento está em range válido (>= 0)."""
    
    @property
    def name(self) -> str:
        return "producao_valida"
    
    def validate(self, df: pd.DataFrame) -> List[ValidationError]:
        errors = []
        
        prod_keywords = ['PRODUCAO', 'PRODUTIVIDADE', 'RENDIMENTO', 'SAFRA', 'COLHEITA', 'TONELADA']
        prod_cols = [col for col in df.columns if any(kw in col.upper() for kw in prod_keywords)]
        
        for col in prod_cols:
            data = pd.to_numeric(df[col], errors='coerce')
            
            negativo = (data < 0) & data.notna()
            count = negativo.sum()
            
            if count > 0:
                errors.append(ValidationError(
                    rule=self.name,
                    column=col,
                    message=f"Coluna '{col}' tem {count} valores negativos",
                    rows_affected=count
                ))
        
        return errors


class AnoValidoRule(ValidationRule):
    """Valida que ano está em range válido (1900 a 2100)."""
    
    @property
    def name(self) -> str:
        return "ano_valido"
    
    def validate(self, df: pd.DataFrame) -> List[ValidationError]:
        errors = []
        
        ano_cols = [col for col in df.columns if 'ANO' in col.upper() or 'YEAR' in col.upper()]
        
        for col in ano_cols:
            data = pd.to_numeric(df[col], errors='coerce')
            
            fora_range = ((data < 1900) | (data > 2100)) & data.notna()
            count = fora_range.sum()
            
            if count > 0:
                errors.append(ValidationError(
                    rule=self.name,
                    column=col,
                    message=f"Coluna '{col}' tem {count} valores fora do range (1900-2100)",
                    rows_affected=count
                ))
        
        return errors


class QuantidadeValidaRule(ValidationRule):
    """Valida que quantidades estão em range válido (>= 0)."""
    
    @property
    def name(self) -> str:
        return "quantidade_valida"
    
    def validate(self, df: pd.DataFrame) -> List[ValidationError]:
        errors = []
        
        qtd_keywords = ['QUANTIDADE', 'QTD', 'QTDE', 'VOLUME', 'PESO']
        qtd_cols = [col for col in df.columns if any(kw in col.upper() for kw in qtd_keywords)]
        
        for col in qtd_cols:
            data = pd.to_numeric(df[col], errors='coerce')
            
            negativo = (data < 0) & data.notna()
            count = negativo.sum()
            
            if count > 0:
                errors.append(ValidationError(
                    rule=self.name,
                    column=col,
                    message=f"Coluna '{col}' tem {count} valores negativos",
                    rows_affected=count
                ))
        
        return errors


# =========================
# FUNÇÃO PRINCIPAL
# =========================

def get_validation_rules() -> List[ValidationRule]:
    """Retorna lista de regras de validação para dados de agricultura."""
    return [
        # Regras básicas
        NotEmptyRule(),
        
        # Regras específicas de agricultura
        AreaValidaRule(),
        ProducaoValidaRule(),
        AnoValidoRule(),
        QuantidadeValidaRule(),
    ]


def validate_dataframe(df: pd.DataFrame, fail_on_error: bool = True) -> ValidationResult:
    """
    Valida um DataFrame de dados de agricultura.
    
    Args:
        df: DataFrame a ser validado
        fail_on_error: Se True, levanta exceção em caso de erro
    
    Returns:
        ValidationResult
    """
    config = ValidationConfig(
        pipeline_name="Agricultura",
        fail_on_error=fail_on_error,
    )
    
    engine = ValidationEngine(config)
    rules = get_validation_rules()
    
    return engine.run(df, rules)


def validate_file(file_path: str, fail_on_error: bool = True) -> ValidationResult:
    """
    Valida um arquivo Parquet de dados de agricultura.
    """
    df = pd.read_parquet(file_path)
    return validate_dataframe(df, fail_on_error=fail_on_error)
