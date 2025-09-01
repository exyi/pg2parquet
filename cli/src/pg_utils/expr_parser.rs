use std::{borrow::Cow, cell::RefCell, fmt::{self, Display}, rc::Rc};

use chrono::format;

use crate::rustutils::ArrayDeconstructor;

use super::type_shenanigans::ParsedType;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Expr {
    Unknown(String),
    Identifier(Option<String>, String, bool, bool), // alias, name, al. quoted, name quoted
    Column(Box<Expr>, String, bool),
    ConstStr(String),
    ConstNum(String),
    ConstTyped(String, String),
    ConstBool(bool),
    ConstNull,
    TypeConversion(Box<Expr>, ParsedType),
    UnaryOp(String, Box<Expr>),
    BinaryOp(String, Box<Expr>, Box<Expr>),
    FunctionCall(String, Vec<Expr>),
    ArrayIndex(Box<Expr>, Vec<Expr>),
    OverExpression(Box<Expr>, ()),
    And(Vec<Expr>),
    Or(Vec<Expr>),
    Not(Box<Expr>),
    SubPlan(String),
}

impl Expr {
    pub fn const_str<T: ToString>(value: T) -> Self {
        Expr::ConstStr(value.to_string())
    }
    pub fn const_num<T: ToString>(value: T) -> Self {
        Expr::ConstNum(value.to_string())
    }
    pub fn const_typed<T: ToString>(value: T, typ: &str) -> Self {
        Expr::ConstTyped(value.to_string(), typ.to_string())
    }
    pub fn conv(self, typ: ParsedType) -> Self {
        Expr::TypeConversion(self.boxed(), typ)
    }
    pub fn ident(str: &str) -> Self {
        Expr::Identifier(None, str.to_string(), false, false)
    }
    pub fn ident_quoted(str: &str) -> Self {
        Expr::Identifier(None, str.to_string(), true, false)
    }
    pub fn unary_op(self, op: &str) -> Self {
        Expr::UnaryOp(op.to_string(), self.boxed())
    }
    pub fn binary_op(self, op: &str, right: Expr) -> Self {
        Expr::BinaryOp(op.to_string(), self.boxed(), right.boxed())
    }
    pub fn array_index(self, indices: Vec<Expr>) -> Self {
        Expr::ArrayIndex(self.boxed(), indices)
    }
    pub fn column(self, name: &str) -> Self {
        Expr::Column(self.boxed(), name.to_string(), false)
    }
    pub fn column_quoted(self, name: &str) -> Self {
        Expr::Column(self.boxed(), name.to_string(), true)
    }
    pub fn log_and(self, expr: Expr) -> Self {
        match self {
            Expr::And(mut exprs) => {
                exprs.push(expr);
                Expr::And(exprs)
            },
            _ => Expr::And(vec![self, expr])
        }
    }
    pub fn log_or(self, expr: Expr) -> Self {
        match self {
            Expr::Or(mut exprs) => {
                exprs.push(expr);
                Expr::Or(exprs)
            },
            _ => Expr::Or(vec![self, expr])
        }
    }
    fn log_not(self) -> Self {
        Expr::Not(self.boxed())
    }

    pub fn boxed(self) -> Box<Self> {
        Box::new(self)
    }
}

impl Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    fn write_str(f: &mut fmt::Formatter<'_>, s: &str, quoted: bool) -> fmt::Result {
        if quoted {
            write!(f, "{}", s)
        } else {
            write!(f, "{:?}", s)
        }
    }
        match self {
            Expr::Unknown(s) => write!(f, "UNK({:?})", s),
            Expr::Identifier(None, name, _, quoted) => write_str(f, name, *quoted),
            Expr::Identifier(Some(alias), name, aquoted, quoted) => {
            write_str(f, alias, *aquoted)?;
            write!(f, ".")?;
            write_str(f, name, *quoted)
        },
            Expr::Column(expr, name, quoted) => {
                write!(f, "(")?;
                expr.fmt(f)?;
                write!(f, ").")?;
                write_str(f, name, *quoted)
            },
            Expr::ConstStr(c) => write!(f, "'{}'", c),
            Expr::ConstNum(c) => write!(f, "{}", c),
            Expr::ConstTyped(c, t) => write!(f, "'{}'::{}", c, t),
            Expr::ConstBool(c) => write!(f, "{}", c),
            Expr::ConstNull => write!(f, "NULL"),
            Expr::TypeConversion(expr, parsed_type) => write!(f, "({})::{}", expr, parsed_type),
            Expr::UnaryOp(_, expr) => todo!(),
            Expr::BinaryOp(_, expr, expr1) => todo!(),
            Expr::FunctionCall(_, exprs) => todo!(),
            Expr::ArrayIndex(expr, exprs) => todo!(),
            Expr::OverExpression(expr, _) => todo!(),
            Expr::And(exprs) => todo!(),
            Expr::Or(exprs) => todo!(),
            Expr::Not(expr) => todo!(),
            Expr::SubPlan(_) => todo!(),
        }
    }
}

pub fn parse_expr(expr: &str) -> Result<Expr, String> {
    let (tokens, end) = tokenize_expr(expr)?;
    println!("Tokens: '{}' {:?}", expr, tokens);

    if (end as usize) != expr.len() {
        return Err(format!("Error parsing expression: unexpected end of input at position {}", end));
    }

    parse_expr_from_tokens(tokens)
}

fn parse_expr_from_tokens<'a>(mut tokens: Vec<Token<'a>>) -> Result<Expr, String> {
    tokens = pass_recursive(tokens, &pass_remove_ws);
    let mut parser = Parser::new(tokens, RecursionLimit::new(128), false);
    // parser.panic_errors = true;
    let expr = parser.parse_expr()?;
    parser.check_end(&expr)?;
    Ok(expr)
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct RecursionLimit {
    limit: u32,
    counter: Rc<RefCell<u32>>
}
impl RecursionLimit {
    pub fn new(limit: u32) -> Self {
        Self { limit, counter: Rc::new(RefCell::new(0)) }
    }
    pub fn guard(&self) -> Result<RecursionLimitLifetime, String> {
        let x = *self.counter.borrow();
        if x >= self.limit {
            return Err(format!("Recursion limit {} reached", x));
        }
        *self.counter.borrow_mut() = x + 1;
        Ok(RecursionLimitLifetime { counter: self.counter.clone() })
    }
}
struct RecursionLimitLifetime {
    counter: Rc<RefCell<u32>>
}
impl Drop for RecursionLimitLifetime {
    fn drop(&mut self) {
        let x = self.counter.borrow().saturating_sub(1);
        *self.counter.borrow_mut() = x;
    }
}

struct ParseError {
    message: String,
    token_path: Vec<usize>
}

impl From<ParseError> for String {
    fn from(e: ParseError) -> String {
        format!("Error parsing expression: {} at token path {:?}", e.message, e.token_path)
    }
}

impl From<String> for ParseError {
    fn from(s: String) -> ParseError {
        ParseError { message: s, token_path: vec![] }
    }
}

impl ParseError {
    fn with_position(mut self, pos: usize) -> Self {
        self.token_path.push(pos);
        self
    }
}

struct Parser<'a> {
    tokens_rev: Vec<Token<'a>>,
    ix: usize,
    recursion_limit: RecursionLimit,
    suppress_errors: bool,
    panic_errors: bool,
}

impl<'a> Parser<'a> {
    pub fn new(mut tokens: Vec<Token<'a>>, recursion_limit: RecursionLimit, suppress_errors: bool) -> Self {
        tokens.reverse();
        Self { tokens_rev: tokens, ix: 0, recursion_limit, suppress_errors, panic_errors: false }
    }

    pub fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        let _guard = self.rec_guard()?;
        self.parse_binary_or()
    }

    pub fn check_end<T: std::fmt::Debug>(&self, after_node: &T) -> Result<(), ParseError> {
        if self.is_end() {
            Ok(())
        } else {
            Err(self.make_err(format!("Expected end of input, instead got {:?} (after {:?})", self.tk(), after_node)))
        }
    }

    fn rec_guard(&self) -> Result<RecursionLimitLifetime, ParseError> {
        self.recursion_limit.guard().map_err(|e| ParseError { message: e, token_path: vec![self.ix] })
    }

    fn fix_err(&mut self, x: Result<Expr, ParseError>) -> Result<Expr, ParseError> {
        if self.suppress_errors {
            let str = format!("{}", Token::Parenthesized(self.tokens_rev.clone().into_iter().rev().collect()));
            self.ix += self.tokens_rev.len();
            Ok(Expr::Unknown(str))
        } else {
            x
        }
    }

    fn make_err(&self, msg: String) -> ParseError {
        if self.panic_errors {
            panic!("Parsing error: {}. Position {} ({:?})", msg, self.ix, self.tk());
        }
        ParseError { message: msg, token_path: vec![self.ix] }
    }

    fn tk(&self) -> &Token<'a> {
        &self.tokens_rev.last().unwrap_or(&Token::Space)
    }
    fn tkat(&self, i: usize) -> &Token<'a> {
        &self.tokens_rev.get(self.tokens_rev.len() - i - 1).unwrap_or(&Token::Space)
    }

    fn pop(&mut self) -> Option<Token<'a>> {
        self.ix += 1;
        let r = self.tokens_rev.pop();
        println!("Popped token, next is {:?}", self.tk());
        r
    }

    fn is_token(&self, t: &Token) -> bool { self.tk().eq(t) }

    fn match_token(&mut self, t: &Token) -> bool {
        if self.is_token(t) {
            self.pop();
            true
        } else {
            false
        }
    }

    fn is_end(&self) -> bool { self.tokens_rev.len() == 0 }

    fn parse_binary_or(&mut self) -> Result<Expr, ParseError> {
        // left associative
        let mut left = self.parse_binary_and()?;
        while self.is_token(&Token::Keyword("OR")) {
            self.pop();
            let right = self.parse_binary_and()?;
            left = Expr::Or(vec![left, right]);
        }
        Ok(left)
    }

    fn parse_binary_and(&mut self) -> Result<Expr, ParseError> {
        // left associative
        let mut left = self.parse_binary_not()?;
        while self.is_token(&Token::Keyword("AND")) {
            self.pop();
            let right = self.parse_binary_not()?;
            left = Expr::And(vec![left, right]);
        }
        Ok(left)
    }

    fn parse_binary_not(&mut self) -> Result<Expr, ParseError> {
        if self.is_token(&Token::Keyword("NOT")) {
            let _guard = self.rec_guard()?;
            self.pop();
            let inner = self.parse_binary_not()?;
            Ok(Expr::Not(Box::new(inner)))
        } else {
            self.parse_binary_is()
        }
    }

    fn parse_binary_is(&mut self) -> Result<Expr, ParseError> {
        let left = self.parse_binary_comparison()?;
        if self.is_token(&Token::Keyword("IS")) {
            self.pop();
            let not = if self.is_token(&Token::Keyword("NOT")) {
                self.pop();
                |x| Expr::Not(Box::new(x))
            } else {
                |x| x
            };

            if matches!(self.tk(), Token::Keyword("NULL" | "TRUE" | "FALSE" | "UNKNOWN")) {
                let Some(Token::Keyword(kw)) = self.pop() else { unreachable!() };
                Ok(not(Expr::UnaryOp(format!("IS {}", kw), Box::new(left))))
            } else {
                Err(self.make_err(format!("Unsupported IS {:?}", self.tk())))
            }
        } else {
            Ok(left)
        }
    }

    fn parse_binary_comparison(&mut self) -> Result<Expr, ParseError> {
        let left = self.parse_binary_like()?;
        if matches!(self.tk(), Token::Punctuation("=" | "<>" | "<" | "<=" | ">" | ">=")) {
            let Some(Token::Punctuation(op)) = self.pop() else { unreachable!() };
            let right = self.parse_binary_like()?;
            Ok(Expr::BinaryOp(op.to_owned(), Box::new(left), Box::new(right)))
        } else {
            Ok(left)
        }
    }

    fn parse_binary_like(&mut self) -> Result<Expr, ParseError> {
        let left = self.parse_binary_other()?;
        // BETWEEN IN LIKE ILIKE SIMILAR, seems that it's always replaced by normal binary operators, so we don't need to worry about it now
        Ok(left)
    }

    fn parse_binary_other(&mut self) -> Result<Expr, ParseError> {
        // there are some operator precedence rules in postgres, but explain expression always come in parenthesised form
        let mut left = self.parse_at_tz()?;
        loop {
            match self.tk() {
                Token::Punctuation("=" | "<>" | "<" | "<=" | ">" | ">=" | "[" | "]" | "," | ".") => return Ok(left),

                Token::Punctuation(op) => {
                    let op = (*op).to_owned();
                    self.pop();
                    let right = self.parse_at_tz()?;
                    left = Expr::BinaryOp(op, Box::new(left), Box::new(right));
                },
                _ => return Ok(left)
            }
        }
    }

    fn parse_at_tz(&mut self) -> Result<Expr, ParseError> {
        let left = self.parse_unary()?;
        if matches!(self.tk(), Token::Ident("AT") | Token::Ident("COLLATE")) {
            Err(self.make_err(format!("AT is not supported now")))
        } else {
            Ok(left)
        }
    }

    fn parse_unary(&mut self) -> Result<Expr, ParseError> {
        if matches!(self.tk(), Token::Punctuation(_)) {
            let _guard = self.rec_guard()?;
            let Some(Token::Punctuation(op)) = self.pop() else { unreachable!() };
            match (op, self.parse_unary()?) {
                ("-", Expr::ConstNum(num)) => Ok(Expr::ConstNum(format!("-{}", num))),
                (_, inner) => Ok(Expr::UnaryOp(op.to_owned(), Box::new(inner)))
            }
        } else {
            self.parse_indexer_cast_call()
        }
    }

    fn parse_indexer_cast_call(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_column_access()?;
        loop {
            match self.tk() {
                &Token::Punctuation(".") => {
                    // Allow column access after any postfix expression (index/cast/call)
                    self.pop();
                    let (column, quoted) = match self.pop() {
                        Some(Token::Ident(column)) => (column.to_owned(), false),
                        Some(Token::IdentString(column)) => (column.into_owned(), true),
                        _ => return Err(self.make_err(format!("Expected column name after '.'")))
                    };
                    println!("Column access: {} {}", column, quoted);
                    left = Expr::Column(left.boxed(), column, quoted);
                },
                &Token::Punctuation("[") => {
                    self.pop();
                    let mut index = Vec::new();
                    loop {
                        index.push(self.parse_expr()?);
                        if !self.match_token(&Token::Punctuation(",")) {
                            break;
                        }
                    }
                    if !self.match_token(&Token::Punctuation("]")) {
                        return Err(self.make_err(format!("Expected ']' after index expression, instead got {:?}", self.tk())));
                    }
                    left = Expr::ArrayIndex(Box::new(left), index);
                },

                Token::Punctuation("::") => {
                    self.pop();
                    let type_name = self.parse_type()?;
                    left = Expr::TypeConversion(Box::new(left), type_name);
                },

                Token::Parenthesized(_) => {
                    let Some(Token::Parenthesized(arg_tks)) = self.pop() else { unreachable!() };
                    let _guard = self.rec_guard()?;

                    let mut args = Vec::new();
                    if arg_tks.len() > 0 {
                        let mut parser2 = Parser::new(arg_tks, self.recursion_limit.clone(), self.suppress_errors);
                        parser2.panic_errors = self.panic_errors;
                        loop {
                            args.push(parser2.parse_expr().map_err(|e| e.with_position(self.ix))?);
                            if !parser2.match_token(&Token::Punctuation(",")) {
                                break;
                            }
                        }

                        if !parser2.is_end() {
                            return Err(parser2.make_err(format!("Expected end of argument list, got {:?}", parser2.tk())));
                        }
                    }

                    match &left {
                        Expr::Identifier(None, name, _, /* quoted */ false) =>
                            left = Expr::FunctionCall(name.to_owned(), args),
                        _ => return Err(self.make_err(format!("Unexpected invocation expression of {:?}", left)))
                    }
                }

                _ => return Ok(left)
            }
        }
    }
    
    fn parse_type(&mut self) -> Result<ParsedType, ParseError> {
        let mut type_name = String::new();
        while matches!(self.tk(), Token::Ident(_) | Token::IdentString(_)) { // TODO: separate ident string
            let name = self.pop().unwrap().into_text().unwrap();
            if !type_name.is_empty() { type_name.push(' '); }
            type_name.push_str(name.as_ref());
        }

        let t = if matches!(self.tk(), Token::Parenthesized(_)) {
            let Some(Token::Parenthesized(argtokens)) = self.pop() else { unreachable!() };
            let args: Vec<_> = argtokens.split(|t| matches!(t, Token::Punctuation(","))).collect();
            let args_str = args.into_iter().map(|a| a.into_iter().map(|t| format!("{}", t)).collect::<Vec<_>>().join(" ")).collect::<Vec<String>>();

            ParsedType::from_name_args(type_name, &args_str.iter().map(|a| a.as_str()).collect::<Vec<&str>>(), None)
        } else {
            ParsedType::from_name_args(type_name, &[], None)
        };

        if matches!(self.tk(), Token::Punctuation("[")) {
            // array
            if matches!(self.tkat(1), Token::Punctuation("]")) {
                self.pop();
                self.pop();
                Ok(ParsedType::Array(None, Box::new(t)))
            } else {
                debug_assert!(false);
                Ok(t)
            }
        } else {
            Ok(t)
        }
    }

    fn parse_column_access(&mut self) -> Result<Expr, ParseError> {
        let mut left = self.parse_primary()?;
        while self.is_token(&Token::Punctuation(".")) {
            self.pop();
            let (column, quoted) = match self.pop() {
                Some(Token::Ident(column)) => (column.to_owned(), false),
                Some(Token::IdentString(column)) => (column.into_owned(), true),
                _ => return Err(self.make_err(format!("Expected column name after '.'")))
            };
            println!("Column access: {} {}", column, quoted);
            left = match left {
                Expr::Identifier(None, name, _, name_quoted) => Expr::Identifier(Some(name), column, name_quoted, quoted),
                left => Expr::Column(Box::new(left), column, quoted)
            }
        }

        Ok(left)
    }
    
    fn parse_primary(&mut self) -> Result<Expr, ParseError> {
        let Some(token1) = self.pop() else {
            return Err(self.make_err(format!("Expected expression, got end of input")));
        };

        match token1 {
            Token::Ident(name) => Ok(Expr::Identifier(None, name.to_owned(), false, false)),
            Token::IdentString(name) => Ok(Expr::Identifier(None, name.into_owned(), false, true)),
            Token::ConstString(s) => Ok(Expr::ConstStr(s.into_owned())),
            Token::Keyword("NULL") => Ok(Expr::ConstNull),
            Token::Keyword("TRUE") => Ok(Expr::ConstBool(true)),
            Token::Keyword("FALSE") => Ok(Expr::ConstBool(false)),
            Token::Number(num) => {
                let mut num = num.to_owned();
                if matches!(self.tk(), Token::Punctuation(".")) {
                    self.pop();
                    let Some(Token::Number(fractional)) = self.pop() else {
                        return Err(self.make_err(format!("Expected fractional part after '.'")));
                    };
                    num.push('.');
                    num.push_str(fractional);
                }
                Ok(Expr::ConstNum(num.to_owned()))
            },
            Token::Parenthesized(inner) => {
                let _guard = self.rec_guard()?;

                match &inner[..] {
                    [ Token::Ident(sp @ ("SubPlan" | "InitPlan")), Token::Ident(num) | Token::Number(num) ] =>
                        return Ok(Expr::SubPlan(format!("{} {}", sp, num))),
                    _ => {}
                }

                let mut parser2 = Parser::new(inner, self.recursion_limit.clone(), self.suppress_errors);
                parser2.panic_errors = self.panic_errors;
                let inner_expr = self.fix_err(parser2.parse_expr()).map_err(|e| e.with_position(self.ix))?;
                parser2.check_end(&inner_expr)?;
                Ok(inner_expr)
            },
            _ => Err(self.make_err(format!("Unexpected token {:?}", token1)))
        }
    }


}

fn is_identifier_str(s: &str) -> bool {
    s.len() > 0 && !char::is_digit(s.chars().next().unwrap(), 10) && s.chars().all(|c| char::is_alphanumeric(c) || c == '_')
}

fn is_identifier(t: &Token) -> bool {
    matches!(t, Token::Ident(_) | Token::IdentString(_))
}

fn pass_recursive<'a>(mut tokens: Vec<Token<'a>>, pass: &impl Fn(Vec<Token<'a>>) -> Vec<Token<'a>>) -> Vec<Token<'a>> {
    tokens = pass(tokens);

    for i in 0..tokens.len() {
        let new = match std::mem::replace(&mut tokens[i], Token::Space) {
            Token::Parenthesized(inner) => {
                let new_inner = pass_recursive(inner, pass);
                if new_inner.len() == 1 && matches!(new_inner[0], Token::ParsedExpr(_, _)) {
                    new_inner.into_1()
                } else {
                    Token::Parenthesized(new_inner)
                }
            },
            tk => tk
        };
        tokens[i] = new;
    }

    tokens
}

fn pass_remove_ws<'a>(tokens: Vec<Token<'a>>) -> Vec<Token<'a>> {
    tokens.into_iter().filter(|t| !matches!(t, Token::Space)).collect()
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Token<'a> {
    ConstString(Cow<'a, str>),
    IdentString(Cow<'a, str>),
    Parenthesized(Vec<Token<'a>>),
    Punctuation(&'a str),
    Ident(&'a str),
    Keyword(&'a str),
    Number(&'a str),
    Space,
    ParsedExpr(Expr, Vec<Token<'a>>),
    TypeName(ParsedType)
}

impl<'a> fmt::Display for Token<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Token::ConstString(c) => write!(f, "'{}'", c.replace('\'', "''")),
            Token::IdentString(c) => write!(f, "\"{}\"", c.replace('"', "\"\"")),
            Token::Parenthesized(inner) => {
                write!(f, "(")?;
                for (i, t) in inner.iter().enumerate() {
                    if i > 0 { write!(f, " ")? }
                    write!(f, "{}", t)?
                }
                write!(f, ")")
            },
            Token::Punctuation(c) => write!(f, "{}", c),
            Token::Ident(c) | Token::Keyword(c) | Token::Number(c) => write!(f, "{}", c),
            Token::Space => write!(f, " "),
            Token::ParsedExpr(_e, _) => {
                write!(f, "EXPR{{ {} }}", _e)?;
                Ok(())
            },
            Token::TypeName(t) =>
                write!(f, "{}", t.name())
        }
    }
}

impl<'a> Token<'a> {
    pub fn replace_with(self, expr: Expr) -> Token<'a> {
        Token::ParsedExpr(expr, vec![self])
    }
    pub fn into_text(self) -> Option<Cow<'a, str>> {
        match self {
            Token::ConstString(s) | Token::IdentString(s) => Some(s),
            Token::Punctuation(s) | Token::Ident(s) | Token::Number(s) => Some(Cow::Borrowed(s)),
            _ => None
        }
    }

    pub fn ref_text<'b: 'a>(&'b self) -> Option<&'a str> {
        match self {
            Token::ConstString(s) | Token::IdentString(s) => Some(s),
            Token::Punctuation(s) | Token::Ident(s) | Token::Number(s) => Some(*s),
            _ => None
        }
    }
}

pub fn tokenize_expr<'a>(expr: &'a str) -> Result<(Vec<Token<'a>>, usize), String> {
    let mut tokens: Vec<Token<'a>> = vec![];
    let mut ix: usize = 0;

    while ix < expr.len() {
        let char = expr.as_bytes()[ix] as char;
        match char {
            _ if char::is_whitespace(char) => {
                skip_ws(expr, &mut ix, &mut tokens);
            },
            '\'' | '"' => {
                let value = read_string(&expr, &mut ix)?;
                assert_eq!(expr.as_bytes()[ix - 1] as char, char);
                tokens.push(if char == '"' {
                    Token::IdentString(value)
                } else {
                    Token::ConstString(value)
                });
            },
            '(' => {
                let (subtokens, subix) = tokenize_expr(&expr[ix+1..])?;
                tokens.push(Token::Parenthesized(subtokens));
                ix += subix + 2;
            },
            ')' => {
                return Ok((tokens, ix));
            },
            _ => {
                let class = char_class(char);
                let mut end = ix + 1;
                while end < expr.len() && class == char_class(expr.as_bytes()[end] as char) { //  && !matches!(expr.as_bytes()[end] as char, ' ' | '\t' | '\n' | '\r' | '(' | ')' | '"' | '\'')
                    end += 1;
                }
                let x = std::str::from_utf8(&expr.as_bytes()[ix..end]).unwrap();
                tokens.push(match class {
                    CharClass::Alpha => if is_number(x) {
                        Token::Number(x)
                    } else if is_reserved_keyword(x) {
                        Token::Keyword(x)
                    } else {
                        Token::Ident(x)
                    },
                    _ => Token::Punctuation(x)
                });
                ix = end;
            }
        }
    }
    return Ok((tokens, ix));

    fn is_number(lit: &str) -> bool {
        lit.len() > 0 && lit.chars().all(|c| char::is_digit(c, 10))
    }

    fn is_reserved_keyword(lit: &str) -> bool {
        matches!(lit.to_ascii_uppercase().as_ref(), "IS" | "NOT" | "NULL" | "TRUE" | "FALSE" | "AND" | "OR" | "UNKNOWN" | "AS" | "ARRAY" | "SOME" | "ANY" | "WHEN" | "THEN")
    }

    fn skip_ws(expr: &str, ix: &mut usize, tokens: &mut Vec<Token>) {
        while *ix < expr.as_bytes().len() {
            match expr.as_bytes()[*ix] as char {
                ' ' | '\t' | '\n' | '\r' => {
                    if !tokens.ends_with(&[Token::Space]) {
                        tokens.push(Token::Space);
                    }
                    *ix += 1;
                },
                _ => {
                    return;
                }
            }
        }
    }
}

fn char_class(c: char) -> CharClass {
    if char::is_alphanumeric(c) || c == '_' || c == '$' {
        CharClass::Alpha
    } else if char::is_whitespace(c) {
        CharClass::Whitespace
    }
    // https://www.postgresql.org/docs/current/sql-createoperator.html
    //  + - * / < > = ~ ! @ # % ^ & | ` ? are operator chars
    // there are futher conditions on which characters can be starting ...,
    // but postgres will split all operators anyway in the simplified EXPLAIN expressions,
    // so we don't need to worry about edge cases and just collapse all operator chars to one name
    else if "+-*/<>=~!@#%^&|`?".contains(c) {
        CharClass::Operator
    } else {
        CharClass::Special(c)
    }
}

#[derive(Debug, PartialEq, Eq)]
enum CharClass {
    Alpha,
    Number,
    Whitespace,
    Operator,
    Special(char)
}

fn read_string<'a>(expr_str: &'a str, ix: &mut usize) -> Result<Cow<'a, str>, String> {
    let expr = expr_str.as_bytes();
    let start_char = expr[*ix] as char;
    if start_char != '\'' && start_char != '"' {
        return Err(format!("Expected string to start with a single or double quote, for expr: {}", expr_str));
    }

    *ix += 1;
    let start_index = *ix;
    let mut result = vec![];
    while *ix < expr.len() {
        if expr[*ix] as char == start_char {
            if expr.len() > *ix + 1 && expr[*ix + 1] as char == start_char {
                result.push(start_char as u8);
                *ix += 2;
                continue;
            } else {
                let inner_span = &expr[start_index..*ix];
                *ix += 1;
                return if result == inner_span {
                    Ok(Cow::Borrowed(std::str::from_utf8(inner_span).unwrap()))
                } else {
                    Ok(Cow::Owned(String::from_utf8(result).unwrap()))
                }
            }
        }

        match expr[*ix] as char {
            // '\\' => {
            // 	*ix += 1;
            // 	let c = expr.as_bytes()[*ix] as char;
            // 	match c {
            // 		'n' => result.push('\n' as u8),
            // 		'r' => result.push('\r' as u8),
            // 		't' => result.push('\t' as u8),
            // 		'\\' => result.push('\\' as u8),
            // 		'\'' => result.push('\'' as u8),
            // 		'"' => result.push('"' as u8),
            // 		'u' | 'U' => {
            // 			let size = if c == 'u' { 4 } else { 8 };
            // 			if *ix + size + 1 >= expr.as_bytes().len() {
            // 				return Error(format!("Unterminated unicode escape sequence in expr: {}", expr));
            // 			}
            // 			let value = u32::from_str_radix(str::from_utf8(expr.as_bytes()[*ix+1..*ix+1+size]), 16)?;
            // 			let Some(value) = char::from_u32(i) else {
            // 				return Error(format!("Invalid unicode escape sequence in expr: {}", expr));
            // 			};
            // 			result.extend_from_slice(value.to_string().as_bytes());
            // 		},
            // 		_ => return Error(format!("Unknown escape sequence: \\{}", c))
            // 	}
            // },
            _ => {
                result.push(expr[*ix]);
                *ix += 1;
            }
        }
    }

    return Err(format!("Unterminated string in expr: *{}*", expr_str));
}


#[test]
fn test_read_string() {
    let mut ix = 0;
    assert_eq!(read_string("'teststring' lalalala", &mut ix).unwrap(), "teststring");
    assert_eq!(ix, 12);
    ix = 0;
    assert_eq!(read_string("'test''st''ri''ng''' lalalala", &mut ix).unwrap(), "test'st'ri'ng'");
    ix = 0;
    assert_eq!(read_string("\"te'st\"\"\"", &mut ix).unwrap(), "te'st\"");
    ix = 0;
    assert!(read_string("'teststring", &mut ix).is_err());
    ix = 0;
    match read_string("'ah\"oj'", &mut ix).unwrap() {
        Cow::Borrowed(r) => assert_eq!(r, "ah\"oj"),
        Cow::Owned(_) => unreachable!()
    }
    ix = 0;
    match read_string("'ah''oj'", &mut ix).unwrap() {
        Cow::Owned(r) => assert_eq!(&r, "ah'oj"),
        Cow::Borrowed(_) => unreachable!()
    }
}

fn case_ab() -> Expr { Expr::Identifier(Some("a".to_owned()), "b".to_owned(), false, false) }
#[test] fn test_parse_col() { assert_eq!(parse_expr("tasks_1.required_checkpoints"), Ok(Expr::Identifier(Some("tasks_1".to_owned()), "required_checkpoints".to_owned(), false, false))); }
#[test] fn test_parse_col_q() { assert_eq!(parse_expr("tasks_1.\"required_checkpoints\""), Ok(Expr::Identifier(Some("tasks_1".to_owned()), "required_checkpoints".to_owned(), false, true))); }
#[test] fn test_parse_plus() {
    assert_eq!(parse_expr("a.b"), Ok(case_ab()));
    assert_eq!(parse_expr("a.b +    1"), Ok(Expr::BinaryOp("+".to_owned(), Box::new(case_ab()), Box::new(Expr::ConstNum("1".to_owned())))));
}
#[test] fn test_parse_subplan() {
    let initplan = ||Expr::SubPlan("InitPlan 2".to_owned());
    let subplan = ||Expr::SubPlan("SubPlan 2".to_owned());
    assert_eq!(parse_expr("(a.b = (InitPlan 2).col1)"), Ok(case_ab().binary_op("=", initplan().column("col1"))));
    assert_eq!(parse_expr("(a.b = (SubPlan 2).col1)"), Ok(case_ab().binary_op("=", subplan().column("col1"))));
    assert_eq!(parse_expr("(a.b = (SubPlan 2).\"cOL1\")"), Ok(case_ab().binary_op("=", subplan().column_quoted("cOL1"))));
}
#[test] fn test_parse_string() { assert_eq!(parse_expr("'ah''oj'"), Ok(Expr::const_str("ah'oj"))); }
#[test] fn test_parse_num() {
    assert_eq!(parse_expr("1.2"), Ok(Expr::const_num(1.2)));
    assert_eq!(parse_expr("100000000000000000010101"), Ok(Expr::const_num("100000000000000000010101")));
    assert_eq!(parse_expr("-0.34"), Ok(Expr::const_num(-0.34)));
}

#[test]
fn test_call_index_convert() {
    assert_eq!(parse_expr("fn(1,2,'3')[123.4,12]::text::numeric"), Ok(
        Expr::FunctionCall("fn".to_owned(), vec![Expr::const_num(1), Expr::const_num(2), Expr::ConstStr("3".to_owned())])
            .array_index(vec![Expr::const_num(123.4), Expr::const_num(12)])
            .conv(ParsedType::from_name_args("text".to_owned(), &[], None))
            .conv(ParsedType::from_name_args("numeric".to_owned(), &[], None))
    ));
}

#[test]
fn test_not_true() {
    assert_eq!(parse_expr("NOT TRUE"), Ok(Expr::ConstBool(true).log_not()));
    assert_eq!(parse_expr("NOT (TRUE)"), Ok(Expr::ConstBool(true).log_not()));
}

#[test]
fn test_paren_and_binops() {
    assert_eq!(
        parse_expr("(1 + 2) * 3"),
        Ok(Expr::const_num(1).binary_op("+", Expr::const_num(2)) .binary_op("*", Expr::const_num(3))));
    assert_eq!(
        parse_expr("1 + -2"),
        Ok(Expr::const_num(1).binary_op("+", Expr::const_num(-2))));
}

#[test]
fn test_compare_columns() {
    let left = Expr::Identifier(Some("a".to_owned()), "b".to_owned(), false, false);
    let right = Expr::Identifier(Some("c".to_owned()), "d".to_owned(), false, false);
    assert_eq!(parse_expr("a.b = c.d"), Ok(left.binary_op("=", right)));
}

#[test]
fn test_quoted_identifiers_both_sides() {
    assert_eq!(
        parse_expr("\"A\".\"B\""),
        Ok(Expr::Identifier(Some("A".to_owned()), "B".to_owned(), true, true))
    );
    assert_eq!(
        parse_expr("a.\"b.c\""),
        Ok(Expr::Identifier(Some("a".to_owned()), "b.c".to_owned(), false, true))
    );
}

#[test]
fn test_function_with_column_and_const_args() {
    assert_eq!(
        parse_expr("fn(a.b, 2)"),
        Ok(Expr::FunctionCall(
            "fn".to_owned(),
            vec![case_ab(), Expr::ConstNum("2".to_owned())]
        ))
    );
}

#[test]
fn test_cast_chain_on_ident() {
    let int4 = ParsedType::from_name_args("int4".to_owned(), &[], None);
    let text = ParsedType::from_name_args("text".to_owned(), &[], None);
    assert_eq!(parse_expr("a::int4::text"), Ok(Expr::ident("a").conv(int4).conv(text)));
}

#[test]
fn test_cast_after_identifier_chain() {
    assert_eq!(parse_expr("a.b::text"), Ok(case_ab().conv(ParsedType::from_name_args("text".to_owned(), &[], None))));
}

#[test]
fn test_parenthesized_column_then_access() {
    assert_eq!(parse_expr("(a.b).c"), Ok(case_ab().column("c")));
}

#[test]
fn test_and_only_expression() {
    assert_eq!(
        parse_expr("a.b = 1 AND NOT FALSE"),
        Ok(case_ab().binary_op("=", Expr::const_num(1)).log_and(Expr::ConstBool(false).log_not()))
    );
}

#[test]
fn test_true_is_true() {
    assert_eq!(parse_expr("TRUE IS TRUE"), Ok(Expr::ConstBool(true).unary_op("IS TRUE")));
    assert_eq!(parse_expr("TRUE IS NOT TRUE"), Ok(Expr::ConstBool(true).unary_op("IS TRUE").log_not()));
}

#[test]
fn test_array_index_with_exprs() {
    assert_eq!(
        parse_expr("arr[1+2, (3)]"),
        Ok(Expr::ident("arr").array_index(vec![
            Expr::const_num(1) .binary_op("+", Expr::const_num(2)),
            Expr::const_num(3)
        ]))
    );
}

#[test]
fn test_nested_parens_true() {
    assert_eq!(parse_expr("((TRUE))"), Ok(Expr::ConstBool(true)));
}

#[test]
fn test_zero_arg_function_call_should_parse() {
    assert_eq!(parse_expr("fn()"), Ok(Expr::FunctionCall("fn".to_owned(), vec![])));
}

// #[test]
// fn test_quoted_function_name_should_parse() {
//     assert_eq!(
//         parse_expr("\"f N\"(1)"),
//         Ok(Expr::FunctionCall("f N".to_owned(), vec![Expr::ConstNum("1".to_owned())]))
//     );
// }

// #[test]
// fn test_schema_qualified_function_call_should_parse() {
//     assert_eq!(
//         parse_expr("schema.fn(1)"),
//         Ok(Expr::FunctionCall("schema.fn".to_owned(), vec![Expr::ConstNum("1".to_owned())]))
//     );
// }

#[test]
fn test_array_index_then_column_access() {
    assert_eq!(parse_expr("arr[1].b"), Ok(Expr::ident("arr").array_index(vec![Expr::const_num(1)]).column("b")));
}

#[test]
fn test_like_operator() {
    assert_eq!(parse_expr("a ~~ 'x%'"), Ok(Expr::ident("a").binary_op("~~", Expr::const_str("x%"))));
}

#[test]
fn test_logical_ops() {
    // NOT binds tighter than AND; AND binds tighter than OR
    assert_eq!(
        parse_expr("NOT TRUE AND FALSE"),
        Ok(Expr::ConstBool(true).log_not().log_and(Expr::ConstBool(false)))
    );
    assert_eq!(
        parse_expr("TRUE OR FALSE AND FALSE"),
        Ok(Expr::Or(vec![
            Expr::ConstBool(true),
            Expr::And(vec![Expr::ConstBool(false), Expr::ConstBool(false)])
        ]))
    );
}

#[test]
fn test_is_and_is_not() {
    assert_eq!(parse_expr("a.b IS NULL"), Ok(Expr::UnaryOp("IS NULL".to_owned(), Box::new(case_ab()))));
    assert_eq!(parse_expr("a.b IS NOT FALSE"), Ok(case_ab().unary_op("IS FALSE").log_not()));
    assert_eq!(parse_expr("a.b IS UNKNOWN"), Ok(case_ab().unary_op("IS UNKNOWN")));
}

#[test]
fn test_comparisons() {
    assert_eq!(parse_expr("1 <= 2"), Ok(Expr::const_num(1).binary_op("<=", Expr::const_num(2))));
    assert_eq!(parse_expr("3 <> 4"), Ok(Expr::const_num(3).binary_op("<>", Expr::const_num(4))));
}

#[test]
fn test_casts_parametric_and_array() {
    let varchar5 = ParsedType::from_name_args("varchar".to_owned(), &["5"], None);
    assert_eq!(parse_expr("'x'::varchar(5)"), Ok(Expr::const_str("x").conv(varchar5)));

    let text_plain = ParsedType::from_name_args("text".to_owned(), &[], None);
    let text_array = ParsedType::Array(None, Box::new(text_plain));
    assert_eq!(parse_expr("'x'::text[]"), Ok(Expr::const_str("x").conv(text_array)));

    let varchar5_plain = ParsedType::from_name_args("character varying".to_owned(), &["5"], None);
    let varchar5_array = ParsedType::Array(None, Box::new(varchar5_plain));
    assert_eq!(parse_expr("'x'::character varying(5)[]"), Ok(Expr::const_str("x").conv(varchar5_array)));
}

#[test]
fn test_array_index_and_nested_column() {
    assert_eq!(parse_expr("arr[1, 2]"), Ok(Expr::ident("arr").array_index(vec![Expr::const_num(1), Expr::const_num(2)])));

    assert_eq!(parse_expr("a.b.c"), Ok(case_ab().column("c")));
    assert_eq!(parse_expr("a.b.\"c d\""), Ok(case_ab().column_quoted("c d")));
}
