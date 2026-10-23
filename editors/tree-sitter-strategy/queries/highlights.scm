(comment) @comment

(string) @string
(escape_sequence) @string.escape

(number) @number
(boolean) @boolean
(ref) @variable.special

(keyword) @keyword
(type) @type

(label (identifier) @label)
(identifier) @variable

(operator) @operator
[ "(" ")" "[" "]" "{" "}" "," ":" "?" ] @punctuation
