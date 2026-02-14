"""
Validação - SOLOS
=================

Regras específicas para dados de solos.
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
# REGRAS ESPECÍFICAS SOLOS
# =========================

class PHValidoRule(ValidationRule):
    """Valida que pH está entre 0 e 14."""
    
    @property
    def name(self) -> str:
        return "ph_valido"
    
    def validate(self, df: pd.DataFrame) -> List[ValidationError]:
        errors = []
        
        ph_cols = [col for col in df.columns if 'PH' in col.upper()]
        
        for col in ph_cols:
            data = pd.to_numeric(df[col], errors='coerce')
            
            fora_range = ((data < 0) | (data > 14)) & data.notna()
            count = fora_range.sum()
            
            if count > 0:
                errors.append(ValidationError(
                    rule=self.name,
                    column=col,
                    message=f"Coluna '{col}' tem {count} valores fora do range (0-14)",
                    rows_affected=count
                ))
        
        return errors


class ProfundidadeValidaRule(ValidationRule):
    """Valida que profundidade está em range válido (0 a 500 cm)."""
    
    @property
    def name(self) -> str:
        return "profundidade_valida"
    
    def validate(self, df: pd.DataFrame) -> List[ValidationError]:
        errors = []
        
        prof_cols = [col for col in df.columns if 'PROF' in col.upper() or 'DEPTH' in col.upper()]
        
        for col in prof_cols:
            data = pd.to_numeric(df[col], errors='coerce')
            
            fora_range = ((data < 0) | (data > 500)) & data.notna()
            count = fora_range.sum()
            
            if count > 0:
                errors.append(ValidationError(
                    rule=self.name,
                    column=col,
                    message=f"Coluna '{col}' tem {count} valores fora do range (0-500 cm)",
                    rows_affected=count,
                    severity="WARNING"
                ))
        
        return errors


class PercentualValidoRule(ValidationRule):
    """Valida que valores percentuais estão entre 0 e 100."""
    
    @property
    def name(self) -> str:
        return "percentual_valido"
    
    def validate(self, df: pd.DataFrame) -> List[ValidationError]:
        errors = []
        
        # Colunas que tipicamente são percentuais em dados de solo
        pct_keywords = ['AREIA', 'ARGILA', 'SILTE', 'SAND', 'CLAY', 'SILT', 'MATERIA_ORGANICA', 'MO', 'UMIDADE']
        
        pct_cols = [col for col in df.columns if any(kw in col.upper() for kw in pct_keywords)]
        
        for col in pct_cols:
            data = pd.to_numeric(df[col], errors='coerce')
            
            fora_range = ((data < 0) | (data > 100)) & data.notna()
            count = fora_range.sum()
            
            if count > 0:
                errors.append(ValidationError(
                    rule=self.name,
                    column=col,
                    message=f"Coluna '{col}' tem {count} valores fora do range (0-100%)",
                    rows_affected=count
                ))
        
        return errors


# =========================
# FUNÇÃO PRINCIPAL
# =========================

def get_validation_rules() -> List[ValidationRule]:
    """Retorna lista de regras de validação para dados de solos."""
    return [
        # Regras básicas
        NotEmptyRule(),
        
        # Regras específicas de solos
        PHValidoRule(),
        ProfundidadeValidaRule(),
        PercentualValidoRule(),
    ]


def validate_dataframe(df: pd.DataFrame, fail_on_error: bool = True) -> ValidationResult:
    """
    Valida um DataFrame de dados de solos.
    
    Args:
        df: DataFrame a ser validado
        fail_on_error: Se True, levanta exceção em caso de erro
    
    Returns:
        ValidationResult
    """
    config = ValidationConfig(
        pipeline_name="Solos",
        fail_on_error=fail_on_error,
    )
    
    engine = ValidationEngine(config)
    rules = get_validation_rules()
    
    return engine.run(df, rules)


def validate_file(file_path: str, fail_on_error: bool = True) -> ValidationResult:
    """
    Valida um arquivo Parquet de dados de solos.
    """
    df = pd.read_parquet(file_path)
    return validate_dataframe(df, fail_on_error=fail_on_error)
