"""
Engine de Validação - Lógica Genérica Reutilizável
==================================================

Responsabilidades:
- Definir interface de validação
- Executar regras de validação
- Coletar e reportar erros
- Gerar relatório de qualidade

Uso:
    from src.validation.engine import ValidationEngine, ValidationRule
    
    class MinhaRegra(ValidationRule):
        def validate(self, df): ...
    
    engine = ValidationEngine(config)
    result = engine.run(df, [MinhaRegra(), ...])
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from datetime import datetime
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("validation")


# =========================
# RESULTADO DE VALIDAÇÃO
# =========================

@dataclass
class ValidationError:
    """Representa um erro de validação."""
    rule: str
    column: Optional[str]
    message: str
    severity: str = "ERROR"  # ERROR, WARNING
    rows_affected: int = 0
    sample_values: List[Any] = field(default_factory=list)


@dataclass
class ValidationResult:
    """Resultado consolidado da validação."""
    is_valid: bool = True
    total_rows: int = 0
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[ValidationError] = field(default_factory=list)
    stats: Dict[str, Any] = field(default_factory=dict)
    executed_at: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def add_error(self, error: ValidationError):
        if error.severity == "WARNING":
            self.warnings.append(error)
        else:
            self.errors.append(error)
            self.is_valid = False
    
    @property
    def error_count(self) -> int:
        return len(self.errors)
    
    @property
    def warning_count(self) -> int:
        return len(self.warnings)
    
    def summary(self) -> str:
        status = "✅ VÁLIDO" if self.is_valid else "❌ INVÁLIDO"
        return (
            f"{status} | "
            f"Linhas: {self.total_rows} | "
            f"Erros: {self.error_count} | "
            f"Avisos: {self.warning_count}"
        )
    
    def to_dict(self) -> Dict:
        return {
            "is_valid": self.is_valid,
            "total_rows": self.total_rows,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "errors": [
                {"rule": e.rule, "column": e.column, "message": e.message, "rows_affected": e.rows_affected}
                for e in self.errors
            ],
            "warnings": [
                {"rule": w.rule, "column": w.column, "message": w.message, "rows_affected": w.rows_affected}
                for w in self.warnings
            ],
            "stats": self.stats,
            "executed_at": self.executed_at,
        }


# =========================
# REGRA DE VALIDAÇÃO (INTERFACE)
# =========================

class ValidationRule(ABC):
    """
    Interface para regras de validação.
    
    Cada regra implementa:
    - name: Nome da regra
    - validate(): Executa a validação e retorna erros
    """
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Nome da regra."""
        pass
    
    @abstractmethod
    def validate(self, df: pd.DataFrame) -> List[ValidationError]:
        """
        Executa validação no DataFrame.
        
        Returns:
            Lista de ValidationError (vazia se válido)
        """
        pass


# =========================
# REGRAS GENÉRICAS PRONTAS
# =========================

class NotEmptyRule(ValidationRule):
    """Valida que o DataFrame não está vazio."""
    
    @property
    def name(self) -> str:
        return "not_empty"
    
    def validate(self, df: pd.DataFrame) -> List[ValidationError]:
        if df.empty:
            return [ValidationError(
                rule=self.name,
                column=None,
                message="DataFrame está vazio",
                rows_affected=0
            )]
        return []


class RequiredColumnsRule(ValidationRule):
    """Valida que colunas obrigatórias existem."""
    
    def __init__(self, columns: List[str]):
        self.columns = columns
    
    @property
    def name(self) -> str:
        return "required_columns"
    
    def validate(self, df: pd.DataFrame) -> List[ValidationError]:
        errors = []
        missing = [col for col in self.columns if col not in df.columns]
        
        if missing:
            errors.append(ValidationError(
                rule=self.name,
                column=None,
                message=f"Colunas obrigatórias ausentes: {missing}",
                rows_affected=len(df)
            ))
        return errors


class NotNullRule(ValidationRule):
    """Valida que colunas não têm valores nulos."""
    
    def __init__(self, columns: List[str]):
        self.columns = columns
    
    @property
    def name(self) -> str:
        return "not_null"
    
    def validate(self, df: pd.DataFrame) -> List[ValidationError]:
        errors = []
        
        for col in self.columns:
            if col not in df.columns:
                continue
                
            null_count = df[col].isna().sum()
            if null_count > 0:
                errors.append(ValidationError(
                    rule=self.name,
                    column=col,
                    message=f"Coluna '{col}' contém {null_count} valores nulos",
                    rows_affected=null_count
                ))
        return errors


class NumericRangeRule(ValidationRule):
    """Valida que valores numéricos estão dentro de um range."""
    
    def __init__(self, column: str, min_value: float = None, max_value: float = None):
        self.column = column
        self.min_value = min_value
        self.max_value = max_value
    
    @property
    def name(self) -> str:
        return "numeric_range"
    
    def validate(self, df: pd.DataFrame) -> List[ValidationError]:
        errors = []
        
        if self.column not in df.columns:
            return errors
        
        col_data = pd.to_numeric(df[self.column], errors='coerce')
        
        if self.min_value is not None:
            below_min = (col_data < self.min_value).sum()
            if below_min > 0:
                errors.append(ValidationError(
                    rule=self.name,
                    column=self.column,
                    message=f"Coluna '{self.column}' tem {below_min} valores abaixo de {self.min_value}",
                    rows_affected=below_min
                ))
        
        if self.max_value is not None:
            above_max = (col_data > self.max_value).sum()
            if above_max > 0:
                errors.append(ValidationError(
                    rule=self.name,
                    column=self.column,
                    message=f"Coluna '{self.column}' tem {above_max} valores acima de {self.max_value}",
                    rows_affected=above_max
                ))
        
        return errors


class NoDuplicatesRule(ValidationRule):
    """Valida que não há linhas duplicadas."""
    
    def __init__(self, columns: List[str] = None):
        self.columns = columns  # None = todas as colunas
    
    @property
    def name(self) -> str:
        return "no_duplicates"
    
    def validate(self, df: pd.DataFrame) -> List[ValidationError]:
        errors = []
        
        subset = self.columns if self.columns else None
        duplicates = df.duplicated(subset=subset).sum()
        
        if duplicates > 0:
            cols_desc = self.columns if self.columns else "todas as colunas"
            errors.append(ValidationError(
                rule=self.name,
                column=None,
                message=f"Encontradas {duplicates} linhas duplicadas (baseado em: {cols_desc})",
                rows_affected=duplicates
            ))
        
        return errors


class DataTypeRule(ValidationRule):
    """Valida tipos de dados das colunas."""
    
    def __init__(self, column: str, expected_type: str):
        """
        Args:
            column: Nome da coluna
            expected_type: 'numeric', 'string', 'datetime'
        """
        self.column = column
        self.expected_type = expected_type
    
    @property
    def name(self) -> str:
        return "data_type"
    
    def validate(self, df: pd.DataFrame) -> List[ValidationError]:
        errors = []
        
        if self.column not in df.columns:
            return errors
        
        col_data = df[self.column]
        
        if self.expected_type == 'numeric':
            non_numeric = pd.to_numeric(col_data, errors='coerce').isna() & col_data.notna()
            invalid_count = non_numeric.sum()
            
            if invalid_count > 0:
                errors.append(ValidationError(
                    rule=self.name,
                    column=self.column,
                    message=f"Coluna '{self.column}' tem {invalid_count} valores não numéricos",
                    rows_affected=invalid_count,
                    sample_values=col_data[non_numeric].head(5).tolist()
                ))
        
        elif self.expected_type == 'datetime':
            non_datetime = pd.to_datetime(col_data, errors='coerce').isna() & col_data.notna()
            invalid_count = non_datetime.sum()
            
            if invalid_count > 0:
                errors.append(ValidationError(
                    rule=self.name,
                    column=self.column,
                    message=f"Coluna '{self.column}' tem {invalid_count} valores de data inválidos",
                    rows_affected=invalid_count,
                    sample_values=col_data[non_datetime].head(5).tolist()
                ))
        
        return errors


# =========================
# VALIDATION ENGINE
# =========================

@dataclass
class ValidationConfig:
    """Configuração da engine de validação."""
    pipeline_name: str
    fail_on_error: bool = True  # Se True, levanta exceção em caso de erro
    log_warnings: bool = True


class ValidationEngine:
    """
    Engine genérica de validação.
    
    Uso:
        config = ValidationConfig(pipeline_name="Clima")
        engine = ValidationEngine(config)
        
        rules = [
            NotEmptyRule(),
            RequiredColumnsRule(["DATA", "HORA", "TEMPERATURA"]),
            NotNullRule(["DATA"]),
            NumericRangeRule("TEMPERATURA", min_value=-40, max_value=50),
        ]
        
        result = engine.run(df, rules)
    """
    
    def __init__(self, config: ValidationConfig):
        self.config = config
    
    def run(self, df: pd.DataFrame, rules: List[ValidationRule]) -> ValidationResult:
        """
        Executa todas as regras de validação.
        
        Args:
            df: DataFrame a ser validado
            rules: Lista de regras de validação
        
        Returns:
            ValidationResult com erros e estatísticas
        """
        result = ValidationResult(total_rows=len(df))
        
        logger.info(f"{'='*50}")
        logger.info(f"VALIDANDO {self.config.pipeline_name}")
        logger.info(f"Linhas: {len(df)} | Colunas: {len(df.columns)}")
        logger.info(f"Regras: {len(rules)}")
        logger.info(f"{'='*50}")
        
        # Coleta estatísticas básicas
        result.stats = {
            "columns": list(df.columns),
            "row_count": len(df),
            "column_count": len(df.columns),
            "null_counts": df.isna().sum().to_dict(),
        }
        
        # Executa cada regra
        for rule in rules:
            try:
                errors = rule.validate(df)
                
                for error in errors:
                    result.add_error(error)
                    
                    if error.severity == "ERROR":
                        logger.error(f"[{rule.name}] {error.message}")
                    elif self.config.log_warnings:
                        logger.warning(f"[{rule.name}] {error.message}")
                
                if not errors:
                    logger.info(f"[{rule.name}] ✅ OK")
                    
            except Exception as e:
                logger.error(f"[{rule.name}] Erro ao executar regra: {e}")
                result.add_error(ValidationError(
                    rule=rule.name,
                    column=None,
                    message=f"Erro ao executar regra: {e}"
                ))
        
        # Resumo
        logger.info(f"{'='*50}")
        logger.info(f"RESULTADO: {result.summary()}")
        logger.info(f"{'='*50}")
        
        # Falha se configurado
        if not result.is_valid and self.config.fail_on_error:
            raise ValidationException(result)
        
        return result


class ValidationException(Exception):
    """Exceção levantada quando validação falha."""
    
    def __init__(self, result: ValidationResult):
        self.result = result
        super().__init__(f"Validação falhou: {result.error_count} erros encontrados")