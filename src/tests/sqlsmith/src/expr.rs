// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use itertools::Itertools;
use rand::seq::SliceRandom;
use rand::Rng;
use risingwave_expr::expr::AggKind;
use risingwave_frontend::expr::{
    agg_func_sigs, func_sigs, AggFuncSig, DataTypeName, ExprType, FuncSign,
};
use risingwave_sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, Ident, ObjectName,
    TrimWhereField, UnaryOperator, Value,
};

use crate::SqlGenerator;

lazy_static::lazy_static! {
    static ref FUNC_TABLE: HashMap<DataTypeName, Vec<FuncSign>> = {
        init_op_table()
    };
}

lazy_static::lazy_static! {
    static ref AGG_FUNC_TABLE: HashMap<DataTypeName, Vec<AggFuncSig>> = {
        init_agg_table()
    };
}

fn init_op_table() -> HashMap<DataTypeName, Vec<FuncSign>> {
    let mut funcs = HashMap::<DataTypeName, Vec<FuncSign>>::new();
    func_sigs().for_each(|func| funcs.entry(func.ret_type).or_default().push(func.clone()));
    funcs
}

fn init_agg_table() -> HashMap<DataTypeName, Vec<AggFuncSig>> {
    let mut funcs = HashMap::<DataTypeName, Vec<AggFuncSig>>::new();
    agg_func_sigs().for_each(|func| funcs.entry(func.ret_type).or_default().push(func.clone()));
    funcs
}

impl<'a, R: Rng> SqlGenerator<'a, R> {
    /// can_agg    - In generating expression, there is two execution mode
    ///                 1)  Non-Aggregate/Aggregate of Groupby Coloumns AND/OR Aggregate of Coloumns
    ///                     that is not Groupby coloumns.                 
    ///                 2)  No Aggregate for all columns (NonGroupby coloumns when group by is being
    ///                     used will not be selected at all in this mode).
    ///
    /// When can_agg is false, it means the second execution mode which is strictly no aggregate for
    /// all coloumns.
    ///
    /// inside_agg - Rule: an aggregate function cannot be inside an aggregate function.
    ///              Since expression can be recursive, so this variable show that currently this
    ///              expression is inside an aggregate function, ensuring the rule is followed.
    pub(crate) fn gen_expr(&mut self, typ: DataTypeName, can_agg: bool, inside_agg: bool) -> Expr {
        if !self.can_recurse() {
            // Stop recursion with a simple scalar or column.
            return match self.rng.gen_bool(0.5) {
                true => self.gen_simple_scalar(typ),
                false => self.gen_col(typ, inside_agg),
            };
        }

        // It is impossible to have can_agg = false and inside_agg = true.
        // It makes no sense that when stricly no aggregate can be used, and this expr is inside an
        // aggregate function.
        assert!(can_agg || !inside_agg);

        // TODO:  https://github.com/singularity-data/risingwave/issues/3989.
        // After the issue is resolved, uncomment the statement below
        // let range = if can_agg & !inside_agg { 99 } else { 90 };
        let range = 90;

        match self.rng.gen_range(0..=range) {
            0..=90 => self.gen_func(typ, can_agg, inside_agg),
            91..=99 => self.gen_agg(typ),
            // TODO: There are more that are not in the functions table, e.g. CAST.
            // We will separately generate them.
            _ => unreachable!(),
        }
    }

    fn gen_col(&mut self, typ: DataTypeName, inside_agg: bool) -> Expr {
        let columns = if inside_agg {
            if self.bound_relations.is_empty() {
                return self.gen_simple_scalar(typ);
            }
            self.bound_relations
                .choose(self.rng)
                .unwrap()
                .get_qualified_columns()
        } else {
            if self.bound_columns.is_empty() {
                return self.gen_simple_scalar(typ);
            }
            self.bound_columns.clone()
        };

        let matched_cols = columns
            .iter()
            .filter(|col| col.data_type == typ)
            .collect::<Vec<_>>();
        if matched_cols.is_empty() {
            self.gen_simple_scalar(typ)
        } else {
            let col_def = matched_cols.choose(&mut self.rng).unwrap();
            Expr::Identifier(Ident::new(&col_def.name))
        }
    }

    fn gen_func(&mut self, ret: DataTypeName, can_agg: bool, inside_agg: bool) -> Expr {
        let funcs = match FUNC_TABLE.get(&ret) {
            None => return self.gen_simple_scalar(ret),
            Some(funcs) => funcs,
        };
        let func = funcs.choose(&mut self.rng).unwrap();
        let exprs: Vec<Expr> = func
            .inputs_type
            .iter()
            .map(|t| self.gen_expr(*t, can_agg, inside_agg))
            .collect();
        let expr = if exprs.len() == 1 {
            make_unary_op(func.func, &exprs[0])
        } else if exprs.len() == 2 {
            make_bin_op(func.func, &exprs)
        } else {
            None
        };
        expr.or_else(|| make_general_expr(func.func, exprs))
            .unwrap_or_else(|| self.gen_simple_scalar(ret))
    }

    fn gen_agg(&mut self, ret: DataTypeName) -> Expr {
        let funcs = match AGG_FUNC_TABLE.get(&ret) {
            None => return self.gen_simple_scalar(ret),
            Some(funcs) => funcs,
        };
        let func = funcs.choose(&mut self.rng).unwrap();

        // Common sense that the aggregation is allowed in the overall expression
        let can_agg = true;
        // show then the expression inside this function is in aggregate function
        let inside_agg = true;
        let expr: Vec<Expr> = func
            .inputs_type
            .iter()
            .map(|t| self.gen_expr(*t, can_agg, inside_agg))
            .collect();
        assert!(expr.len() == 1);

        let distinct = self.flip_coin();
        make_agg_expr(func.func.clone(), expr[0].clone(), distinct)
            .unwrap_or_else(|| self.gen_simple_scalar(ret))
    }
}

fn make_unary_op(func: ExprType, expr: &Expr) -> Option<Expr> {
    use {ExprType as E, UnaryOperator as U};
    let unary_op = match func {
        E::Neg => U::Minus,
        E::Not => U::Not,
        E::BitwiseNot => U::PGBitwiseNot,
        _ => return None,
    };
    Some(Expr::UnaryOp {
        op: unary_op,
        expr: Box::new(expr.clone()),
    })
}

fn make_general_expr(func: ExprType, exprs: Vec<Expr>) -> Option<Expr> {
    use ExprType as E;

    match func {
        E::Trim | E::Ltrim | E::Rtrim => Some(make_trim(func, exprs)),
        E::IsNull => Some(Expr::IsNull(Box::new(exprs[0].clone()))),
        E::IsNotNull => Some(Expr::IsNotNull(Box::new(exprs[0].clone()))),
        E::IsTrue => Some(Expr::IsTrue(Box::new(exprs[0].clone()))),
        E::IsNotTrue => Some(Expr::IsNotTrue(Box::new(exprs[0].clone()))),
        E::IsFalse => Some(Expr::IsFalse(Box::new(exprs[0].clone()))),
        E::IsNotFalse => Some(Expr::IsNotFalse(Box::new(exprs[0].clone()))),
        E::Position => Some(Expr::Function(make_simple_func("position", &exprs))),
        E::RoundDigit => Some(Expr::Function(make_simple_func("round", &exprs))),
        E::Repeat => Some(Expr::Function(make_simple_func("repeat", &exprs))),
        E::CharLength => Some(Expr::Function(make_simple_func("char_length", &exprs))),
        E::Substr => Some(Expr::Function(make_simple_func("substr", &exprs))),
        E::Length => Some(Expr::Function(make_simple_func("length", &exprs))),
        E::Upper => Some(Expr::Function(make_simple_func("upper", &exprs))),
        E::Lower => Some(Expr::Function(make_simple_func("lower", &exprs))),
        E::Replace => Some(Expr::Function(make_simple_func("replace", &exprs))),
        E::Md5 => Some(Expr::Function(make_simple_func("md5", &exprs))),
        E::ToChar => Some(Expr::Function(make_simple_func("to_char", &exprs))),
        E::Overlay => Some(make_overlay(exprs)),
        _ => None,
    }
}

// Even though when all current AggKind is implemented, the reason that Option is used here
// because of if future there is a new AggKind is added, it would cause the compilation error here
// So it is still better to keep it as Option
fn make_agg_expr(func: AggKind, expr: Expr, distinct: bool) -> Option<Expr> {
    use AggKind as A;

    match func {
        A::Sum => Some(Expr::Function(make_agg_func("sum", &[expr], distinct))),
        A::Min => Some(Expr::Function(make_agg_func("min", &[expr], distinct))),
        A::Max => Some(Expr::Function(make_agg_func("max", &[expr], distinct))),
        A::Count => Some(Expr::Function(make_agg_func("count", &[expr], distinct))),
        A::Avg => Some(Expr::Function(make_agg_func("avg", &[expr], distinct))),
        // Refer to src/frontend/src/optimizer/plan_node/logical_agg.rs line  356 , it is todo:
        // Uncomment the line below when it is implemented
        // A::StringAgg => Expr::Function(make_agg_func("string_agg", &[expr], distinct)),
        A::SingleValue => Some(Expr::Function(make_agg_func(
            "single_value",
            &[expr],
            false,
        ))),
        A::ApproxCountDistinct => Some(Expr::Function(make_agg_func(
            "approx_count_distinct",
            &[expr],
            false,
        ))),
        _ => None,
    }
}

fn make_trim(func: ExprType, exprs: Vec<Expr>) -> Expr {
    use ExprType as E;

    let trim_type = match func {
        E::Trim => TrimWhereField::Both,
        E::Ltrim => TrimWhereField::Leading,
        E::Rtrim => TrimWhereField::Trailing,
        _ => unreachable!(),
    };
    let trim_where = if exprs.len() > 1 {
        Some((trim_type, Box::new(exprs[1].clone())))
    } else {
        None
    };
    Expr::Trim {
        expr: Box::new(exprs[0].clone()),
        trim_where,
    }
}

fn make_overlay(exprs: Vec<Expr>) -> Expr {
    if exprs.len() == 3 {
        Expr::Overlay {
            expr: Box::new(exprs[0].clone()),
            new_substring: Box::new(exprs[1].clone()),
            start: Box::new(exprs[2].clone()),
            count: None,
        }
    } else {
        Expr::Overlay {
            expr: Box::new(exprs[0].clone()),
            new_substring: Box::new(exprs[1].clone()),
            start: Box::new(exprs[2].clone()),
            count: Some(Box::new(exprs[3].clone())),
        }
    }
}

/// Generates simple functions such as `length`, `round`, `to_char`. These operate on datums instead
/// of columns / rows.
fn make_simple_func(func_name: &str, exprs: &[Expr]) -> Function {
    let args = exprs
        .iter()
        .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e.clone())))
        .collect();

    Function {
        name: ObjectName(vec![Ident::new(func_name)]),
        args,
        over: None,
        distinct: false,
        order_by: vec![],
        filter: None,
    }
}

/// This is the function that generate aggregate function.
/// DISTINCT , ORDER BY or FILTER is allowed in aggregation functions。
/// Currently, distinct is allowed only, other and others rule is TODO: https://github.com/singularity-data/risingwave/issues/3933
fn make_agg_func(func_name: &str, exprs: &[Expr], distinct: bool) -> Function {
    let args = exprs
        .iter()
        .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e.clone())))
        .collect();

    Function {
        name: ObjectName(vec![Ident::new(func_name)]),
        args,
        over: None,
        distinct,
        order_by: vec![],
        filter: None,
    }
}

fn make_bin_op(func: ExprType, exprs: &[Expr]) -> Option<Expr> {
    use {BinaryOperator as B, ExprType as E};
    let bin_op = match func {
        E::Add => B::Plus,
        E::Subtract => B::Minus,
        E::Multiply => B::Multiply,
        E::Divide => B::Divide,
        E::Modulus => B::Modulo,
        E::GreaterThan => B::Gt,
        E::GreaterThanOrEqual => B::GtEq,
        E::LessThan => B::Lt,
        E::LessThanOrEqual => B::LtEq,
        E::Equal => B::Eq,
        E::NotEqual => B::NotEq,
        E::And => B::And,
        E::Or => B::Or,
        E::Like => B::Like,
        E::BitwiseAnd => B::BitwiseAnd,
        E::BitwiseOr => B::BitwiseOr,
        E::BitwiseXor => B::PGBitwiseXor,
        E::BitwiseShiftLeft => B::PGBitwiseShiftLeft,
        E::BitwiseShiftRight => B::PGBitwiseShiftRight,
        _ => return None,
    };
    Some(Expr::BinaryOp {
        left: Box::new(exprs[0].clone()),
        op: bin_op,
        right: Box::new(exprs[1].clone()),
    })
}

pub(crate) fn sql_null() -> Expr {
    Expr::Value(Value::Null)
}

pub fn print_function_table() -> String {
    let func_str = func_sigs()
        .map(|sign| {
            format!(
                "{:?}({}) -> {:?}",
                sign.func,
                sign.inputs_type
                    .iter()
                    .map(|arg| format!("{:?}", arg))
                    .join(", "),
                sign.ret_type,
            )
        })
        .join("\n");

    let agg_func_str = agg_func_sigs()
        .map(|sign| {
            format!(
                "{:?}({}) -> {:?}",
                sign.func,
                sign.inputs_type
                    .iter()
                    .map(|arg| format!("{:?}", arg))
                    .join(", "),
                sign.ret_type,
            )
        })
        .join("\n");

    func_str + "\n" + &agg_func_str
}
