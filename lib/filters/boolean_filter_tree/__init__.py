import re
import functools

# Config Keywords
from typing import Callable, List

INCLUDE = 'include'
EXCLUDE = 'exclude'
NOT = 'not'
AND = 'and'
OR = 'or'
EQ = 'eq'
NE = 'ne'
GT = 'gt'
LT = 'lt'
GE = 'ge'
LE = 'le'
MATCHES = 'matches'
STARTSWITH = 'startswith'
ENDSWITH = 'endswith'
COLUMN = 'column'
NAME = 'name'


def _or(df, a, b):
    return a(df) | b(df)


def _or_all(rules: List[Callable], df):
    p: Callable = functools.partial(_or, df)
    return functools.reduce(p, rules)


def _and(df, a, b):
    return a(df) & b(df)


def _and_all(rules: List[Callable], df):
    p: Callable = functools.partial(_and, df)
    return functools.reduce(p, rules)


def _ne(column, value, df):
    t = type(value)
    return df[column].astype(t) != value


def _eq(column, value, df):
    t = type(value)
    return df[column].astype(t) == value


def _gt(column, value, df):
    t = type(value)
    return df[column].astype(t) > value


def _lt(column, value, df):
    t = type(value)
    return df[column].astype(t) < value


def _ge(column, value, df):
    t = type(value)
    return df[column].astype(t) >= value


def _le(column, value, df):
    t = type(value)
    return df[column].astype(t) <= value


def _not_sw(column, s, df):
    return ~df[column].astype(str).str.startswith(s)


def _sw(column, s, df):
    return df[column].astype(str).str.startswith(s)


def _not_ew(column, s, df):
    return ~df[column].astype(str).str.endswith(s)


def _ew(column, s, df):
    return df[column].astype(str).str.endswith(s)


def _doesnt_match(column, pattern, df):
    return ~df[column].astype(str).str.match(pattern)


def _matches(column, pattern, df):
    return df[column].astype(str).str.match(pattern)


class BooleanFilterTree(object):
    def __init__(self, config):
        self.rules = [self.parse_rule(r) for r in config]

    def apply(self, df, inplace=False):
        if (isinstance(df, type(None))) or not len(df):
            return df
        for r in self.rules:
            try:
                # _bf = len(df)
                _df = df.drop(df[~r(df)].index, inplace=inplace)
                df = df if inplace else _df
                # print(f'{r.__rule_name__}: {_bf} => {len(df)}')
            except KeyError as ke:
                raise Exception(
                    "Column in rule '" +
                    r.__rule_name__ +
                    "' not found in dataframe: " +
                    ke.args[0]
                ) from ke
            if not len(df):
                return None if inplace else df
        return None if inplace else df

    def parse_rule(self, r):
        if INCLUDE in r:
            rule = r[INCLUDE]
            rule_lambda = self._parse_rule(rule)
        elif EXCLUDE in r:
            rule = r[EXCLUDE]
            rule_lambda = self._parse_rule(rule, True)
        else:
            raise Exception(f'Only {INCLUDE} and {EXCLUDE} allowed as top-level rules')

        rule_lambda.__rule_name__ = r[NAME]
        return rule_lambda

    def _parse_rule(self, rule, invert=False):
        if OR in rule:
            rules = [self._parse_rule(r) for r in rule[OR]]
            if len(rules) == 1:
                return rules[0]
            return functools.partial(_or_all, rules)
        elif AND in rule:
            rules = [self._parse_rule(r) for r in rule[AND]]
            if len(rules) == 1:
                return rules[0]
            return functools.partial(_and_all, rules)
        else:
            column = rule[COLUMN]
            if EQ in rule:
                value = rule[EQ]
                if invert:
                    return functools.partial(_ne, column, value)
                return functools.partial(_eq, column, value)
            elif NE in rule:
                value = rule[NE]
                if invert:
                    return functools.partial(_eq, column, value)
                return functools.partial(_ne, column, value)
            elif GT in rule:
                value = rule[GT]
                if invert:
                    return functools.partial(_le, column, value)
                return functools.partial(_gt, column, value)
            elif LT in rule:
                value = rule[LT]
                if invert:
                    return functools.partial(_ge, column, value)
                return functools.partial(_lt, column, value)
            elif GE in rule:
                value = rule[GE]
                t = type(value)
                if invert:
                    return functools.partial(_lt, column, value)
                return functools.partial(_ge, column, value)
            elif LE in rule:
                value = rule[LE]
                t = type(value)
                if invert:
                    return functools.partial(_gt, column, value)
                return functools.partial(_le, column, value)
            elif STARTSWITH in rule:
                s = rule[STARTSWITH]
                if invert:
                    return functools.partial(_not_sw, column, s)
                return functools.partial(_sw, column, s)
            elif ENDSWITH in rule:
                s = rule[ENDSWITH]
                if invert:
                    return functools.partial(_not_ew, column, s)
                return functools.partial(_ew, column, s)
            elif MATCHES in rule:
                pattern = re.compile(rule[MATCHES])
                if invert:
                    return functools.partial(_doesnt_match, column, pattern)
                return functools.partial(_matches, column, pattern)
            else:
                raise Exception("Unknown rule: " + str(rule))
