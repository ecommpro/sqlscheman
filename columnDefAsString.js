module.exports = ({
	type,
	dataType,
	colType,
	notnull,
	nullable,
	autoIncrement,
	fixed,
	length,
	precision,
	scale = 0,
	unique,
	unsigned,
	comment,
	custom,
	charset,
	collation,
	check,
	extra,
	...def
}) => [
	[type, length && `(${length})`, precision && `(${precision}, ${scale})`, ].filter(v => v).join(''),
	unsigned && 'unsigned',
	notnull && 'NOT NULL',
	autoIncrement && 'AUTO_INCREMENT',
	def.default === null && `DEFAULT NULL` || typeof def.default !== 'undefined' && `DEFAULT ${def.default}`,
	extra,
	comment && `COMMENT '${comment}'`
]
.filter(v => v)
.join(' ')
;
