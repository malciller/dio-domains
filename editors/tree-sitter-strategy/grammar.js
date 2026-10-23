/**
 * Tree-sitter grammar for Dio strategy scripts (`.strategy`).
 *
 * The language is indentation-scoped and parsed by the engine's own recursive
 * descent parser (`src/engine/strategies/protocol/strategy_script.ml`). This
 * grammar is intentionally flat: it tokenizes the source without trying to
 * reproduce block structure, which is enough for editor features (highlighting,
 * bracket matching) and keeps it from drifting out of sync with the real parser.
 * Diagnostics from `dio strategy lsp` remain the source of truth.
 */
module.exports = grammar({
  name: 'strategy',

  extras: $ => [/\s+/, $.comment],

  word: $ => $.identifier,

  rules: {
    source_file: $ => repeat($._token),

    comment: $ => token(seq(choice('--', '//', '#'), /[^\n]*/)),

    _token: $ => choice(
      $.keyword,
      $.type,
      $.boolean,
      $.ref,
      $.number,
      $.string,
      $.label,
      $.identifier,
      $.operator,
      '(',
      ')',
      '[',
      ']',
      '{',
      '}',
      ',',
      ':',
      '?'
    ),

    keyword: $ => token(prec(1, choice(
      'strategy',
      'version',
      'remembers',
      'tunable',
      'when',
      'if',
      'otherwise',
      'else',
      'stop',
      'let',
      'bind',
      'persist',
      'and',
      'or',
      'not',
    ))),

    type: $ => token(prec(1, choice(
      'bool',
      'int',
      'float',
      'string',
      'decimal_str',
      'range',
      'enum',
      'persist',
      'buy_intent?',
      'sell_intent?',
      'reserve_policy',
      'float?',
    ))),

    boolean: $ => token(prec(1, choice('true', 'false'))),

    ref: $ => token(/\$[a-zA-Z_][a-zA-Z0-9_.]*/),

    number: $ => token(/\d+(\.\d+)?([eE][+-]?\d+)?/),

    string: $ => seq(
      '"',
      repeat(choice(/[^"\\\n]+/, $.escape_sequence)),
      '"',
    ),

    escape_sequence: $ => token(/\\./),

    label: $ => prec(1, seq($.identifier, ':')),

    // Dotted names are single tokens: `cancel.excess.buys` is one name.
    // A trailing `?` is allowed so optional types (`float?`, `buy_intent?`)
    // stay whole words instead of splitting on the `?`.
    identifier: $ => token(/[a-zA-Z_][a-zA-Z0-9_.]*\??/),

    operator: $ => token(choice(
      '==',
      '!=',
      '<=',
      '>=',
      '&&',
      '||',
      '=',
      '+',
      '-',
      '*',
      '/',
      '<',
      '>',
    )),
  },
});
