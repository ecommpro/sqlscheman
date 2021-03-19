const columnDef = require('./columnDefAsString');

const toCamelCase = str => str.toLowerCase().replace(/[^a-zA-Z0-9]+(.)/g, (_, char) => char.toUpperCase());
const camelCaseKeys = o1 => Object.keys(o1).reduce((o2, key) => { o2[toCamelCase(key)] = o1[key]; return o2; }, {})
const camelCaseSortedKeys = o1 => Object.keys(o1).sort().reduce((o2, key) => { o2[toCamelCase(key)] = o1[key]; return o2; }, {})
const pick = (...props) => o => props.reduce((a, e) => ({ ...a, [e]: o[e] }), {});
const pickAsList = (...props) => o => props.reduce((a, e) => ([...a, o[e]]), []);

// Remove dependency on lodash by using our own set method
const set = (obj, path, value) => {
  // Regex explained: https://regexr.com/58j0k
  const pathArray = Array.isArray(path) ? path : path.match(/([^[.\]])+/g)

  pathArray.reduce((acc, key, i) => {
    if (acc[key] === undefined) acc[key] = {}
    if (i === pathArray.length - 1) acc[key] = value
    return acc[key]
  }, obj)
}

const compareColumnDefinition = (current, wanted) => {
	const wantedColumnType = `${wanted.type}${wanted.length && `(${wanted.length})` || ''}`;

	if (
		current.columnType !== wantedColumnType
		|| String(current.columnDefault || 'NULL') !== String(wanted.default == null && 'NULL' || wanted.default)
	) {
		//console.log([String(current.columnDefault || 'NULL'), String(wanted.default || 'NULL')]);
		return false;
	}

	

	return true;
}

const emptyFacts = () => ({
	mustExist: [],
	mustNotExist: [],
});

const sources = {
	tables: {
		query: `
		SELECT * FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME
		`,
		keyFields: ['tableName'],
	},
	
	columns: {
		query: `
		SELECT * FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() ORDER BY TABLE_NAME
		`,
		keyFields: ['tableName', 'columnName'],
	},

	constraints: {
		query: `
		SELECT *
		FROM information_schema.REFERENTIAL_CONSTRAINTS c
		LEFT JOIN information_schema.KEY_COLUMN_USAGE k ON k.CONSTRAINT_NAME = c.CONSTRAINT_NAME AND k.TABLE_NAME = c.TABLE_NAME
		WHERE k.TABLE_SCHEMA = DATABASE()
		ORDER BY c.TABLE_NAME
		`,
		keyFields: ['tableName', 'constraintName', /*'columnName', 'ordinalPosition'*/],
	},
	
	indexes: {
		query: `
		SELECT *
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = DATABASE()
		ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX
		`,
		keyFields: ['tableName', 'indexName', /*'seqInIndex'*/],
	},
};

const keyTypes = { primaryKey: 'primaryKey', key: 'keys', uniqueKey: 'uniqueKeys', foreignKey: 'foreignKeys' };

const scheman = ({ connection }) => {

	let _tables, columns, constraints, indexes;
	let primaryKeys = new Map, keys = new Map, uniqueKeys = new Map, foreignKeys = new Map, objects = new Map;

	let tables = new Map;

	let facts = {
		table: emptyFacts(),
		column: emptyFacts(),
		primaryKey: emptyFacts(),
		uniqueKey: emptyFacts(),
		key: emptyFacts(),
		foreignKey: emptyFacts(),
	}

	let blueprint = {
		createTable: {},
		addColumn: {},
		modifyColumn: {},
	}

	const retrieve = fn => async ({ query, keyFields, ...def }) => (await connection.query(query))
		.map(o => camelCaseSortedKeys(o))
		.reduce((map, row) => fn({ map, row, key: pickAsList(...keyFields)(row).join('.'), ...def }), new Map)
	;

	const quoteId = id => id.split(',').map(id => `\`${id}\``).join(', ');

	const sql = {
		createTable: (def) => {
			const{ tableName, columns = {}, primaryKey, uniqueKeys = {}, keys = {}, foreignKeys = {} } = def

			return `CREATE TABLE \`${tableName}\` (
${[
			...Object.keys(columns).map(columnName => `    \`${columnName}\` ${columnDef(columns[columnName])}`),
			primaryKey && `    PRIMARY KEY (${quoteId(typeof primaryKey === 'string' ? primaryKey : Object.keys(primaryKey)[0])})`,
			...Object.values(uniqueKeys).map(({ id }) => `    UNIQUE KEY (${quoteId(id)})`),
			...Object.values(keys).map(({ id }) => `    KEY (${quoteId(id)})`),
		].filter(v => v).join(',\n')}
)`},

		alterTable: def => {},

		addColumn: ({ tableName, columnName, ...def }) => {
			return `ALTER TABLE \`${tableName}\` ADD COLUMN \`${columnName}\` ${columnDef(def)}`
		},

		modifyColumn: ({ tableName, columnName, ...def }) => {
			return `ALTER TABLE \`${tableName}\` MODIFY COLUMN \`${columnName}\` ${columnDef(def)}`
		},
		
	}

	const fact = Object.keys(facts).reduce((fact, objectType) => ({
		...fact,
		[objectType]: {
			mustExist: (def) => facts[objectType].mustExist.push(def),
			mustNotExist: (def) => facts[objectType].mustNotExist.push(def)
		}
	}), {});
	
	return {
		load: async () => {
			[_tables, columns, constraints, indexes] = await Promise.all([
				...['tables', 'columns'].map(key => sources[key])
				.map(retrieve(({ map, row, key }) => map.set(key, row))),

				...['constraints', 'indexes'].map(key => sources[key])
				.map(retrieve(({ map, row, key }) => map.set(key, [...(map.get(key) || []), row]))),
			])

			_tables.forEach(({ tableName }) => {
				tables.set(tableName, {
					name: tableName,
					columns: {},
					primaryKey: {},
					uniqueKeys: {},
					keys: {},
					foreignKeys: {},
				})
			});
			
			columns.forEach(column => {
				const { tableName, columnName } = column;
				const globalId = `${tableName}:${columnName}`;
				objects.set(`column:${globalId}`, true);
				tables.get(tableName)['columns'][columnName] = column;
			});

			indexes.forEach(index => {
				const def = index[0];
				const { tableName } = def;
				const id = index.map(({ columnName }) => columnName).sort().join(',');
				const globalId = `${def.tableName}:${id}`;
				
				if (def.indexName === 'PRIMARY') {
					tables.get(tableName)['primaryKey'][id] = index;
					primaryKeys.set(globalId, index);
					objects.set(`primaryKey:${globalId}`, true);
				} else if (def.nonUnique) {
					tables.get(tableName)['keys'][id] = index;
					keys.set(globalId, index);
					objects.set(`key:${globalId}`, true);
				} else {
					tables.get(tableName)['uniqueKeys'][id] = index;
					uniqueKeys.set(globalId, index);
					objects.set(`uniqueKey:${globalId}`, true);
				}
			});

			constraints.forEach(constraint => {
				const def = constraint[0];
				const { tableName } = def;
				const fromColumns = constraint.map(({ columnName }) => columnName).sort().join(',')
				const toColumns = constraint.map(({ referencedColumnName }) => referencedColumnName).sort().join(',')
				const id = `${fromColumns}>${def.referencedTableName}:${toColumns}`;
				const globalId = `${def.tableName}:${id}`;
				tables.get(tableName)['foreignKeys'][id] = constraint;
				foreignKeys.set(globalId, constraint);
				objects.set(`foreignKey:${globalId}`, true);
			});

			return {
				tables, columns, constraints, indexes, primaryKeys, uniqueKeys, foreignKeys, objects
			}
		},

		createTable: sql.createTable,

		fact,
		facts,
		
		queriesNeededToMaterializeFacts: () => {

			facts.table.mustExist.forEach(def => {

				const { tableName, columns = {}, primaryKey, keys = {}, uniqueKeys = {}, foreignKeys = {} } = def;

				if (!tables.get(tableName)) {
					set(blueprint, ['createTable', tableName, 'tableName'], tableName);
				}
				
				for (const [columnName, def] of Object.entries(columns)) {
					facts.column.mustExist.push({ tableName, columnName, ...def });
				}

				if (primaryKey) {
					if (typeof primaryKey === 'string') {
						facts.primaryKey.mustExist.push({ tableName, id: primaryKey });
					} else {
						for (const [id, def] of Object.entries(primaryKey)) {
							facts.primaryKey.mustExist.push({ tableName, id, ...def });
						}
					}
				}

				for (const [objectType, items] of Object.entries({ key: keys, uniqueKey: uniqueKeys, foreignKey: foreignKeys })) {
					for (const [id, def] of Object.entries(items)) {
						facts[objectType].mustExist.push({ tableName, id, ...def });
					}
				}

			})

			facts.column.mustExist.forEach(def => {

				const { tableName, columnName } = def;
				const tableDef = tables.get(tableName);
				
				if (tableDef) {
					if (tableDef.columns[columnName]) {
						if (!compareColumnDefinition(tableDef.columns[columnName], def)) {
							set(blueprint, ['modifyColumn', tableName, columnName], def);
						}
					} else {
						set(blueprint, ['addColumn', tableName, columnName], def);
					}
				} else {
          set(blueprint, ['createTable', tableName, 'tableName'], tableName);
					set(blueprint, ['createTable', tableName, 'columns', columnName], def);
				}
				
			})

			//console.log(facts.foreignKey);

			Object.entries(keyTypes).map(([keyType, keyTypeKey]) => {
				facts[keyType].mustExist.forEach(def => {
					
					const { tableName, id } = def;
					const tableDef = tables.get(tableName);
	
					if (tableDef) {
						const { [keyTypeKey]: _keys } = tableDef
						const keys = typeof _keys === 'string' ? { [keyType]: { [_keys]: {} } } : _keys;

						if (id in keys) {
							// check for differences
							// WARNING: caution with primary key: there can be only one
						} else {
							set(blueprint, [`addKey:${keyType}`, tableName, id], def);
						}
	
					} else {
            set(blueprint, ['createTable', tableName, 'tableName'], tableName);
						set(blueprint, ['createTable', tableName, keyTypeKey, id], def);
					}

				})
			});

			let queries = [];

			for (const { tableName, ...def } of Object.values(blueprint.createTable)) {
				tableName && queries.push(sql.createTable({ tableName, ...def }));
			}
			
			for (const [tableName, columns] of Object.entries(blueprint.addColumn)) {
				for (const [columnName, def] of Object.entries(columns)) {
					queries.push(sql.addColumn({ tableName, columnName, ...def }));
				}
			}

			for (const [tableName, columns] of Object.entries(blueprint.modifyColumn)) {
				for (const [columnName, def] of Object.entries(columns)) {
					queries.push(sql.modifyColumn({ tableName, columnName, ...def }));
				}
			}

			return queries;
		}

	}
}

module.exports = {
  scheman,
}