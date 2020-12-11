'use strict';

const async = require('async');
const pg = require('pg');
const Transaction = require('../transaction.js');
const NodalPostgresAdapter = require('./postgres.js');

class PostgresAdapter extends NodalPostgresAdapter {

  constructor(db, cfg) {

    super(db, cfg);

    this._config.idleTimeoutMillis = this._config.idleTimeoutMillis || 30000;
    this._config.connectionTimeoutMillis = this._config.connectionTimeoutMillis || 30000;
    this._config.max = this._config.max || 100;

    this.pool = new pg.Pool(this._config);

  }

  close() {

    this.pool.end();

  }

  createTransaction(callback) {

    if (this.pool._isFull()) {
      return callback(new Error('Pool is full'));
    }

    this.pool.connect((err, client, complete) => {
      if (err) {
        complete();
        return callback(err);
      }

      this.beginClient(client, (err) => {

        if (err) {
          complete();
          return callback(err);
        }

        return callback(null, new Transaction(this, client, complete));

      });

    });

  }

  query(query, params, callback) {

    if (arguments.length < 3) {
      throw new Error('.query requires 3 arguments');
    }

    if (!(params instanceof Array)) {
      throw new Error('params must be a valid array');
    }

    if(typeof callback !== 'function') {
      throw new Error('Callback must be a function');
    }

    if (this.pool._isFull()) {
      return callback(new Error('Pool is full'));
    }

    this.pool.connect((err, client, complete) => {

      if (err) {
        this.db.error(err.message);
        complete();
        callback.apply(this, [err]);
        return;
      }

      this.queryClient(client, query, params, (function(err, results) {

        complete();
        callback.apply(this, arguments);

      }).bind(this));

    });

    return true;

  }

  transaction(preparedArray, callback) {

    if (!preparedArray.length) {
      throw new Error('Must give valid array of statements (with or without parameters)');
    }

    if (typeof preparedArray === 'string') {
      preparedArray = preparedArray.split(';').filter(function(v) {
        return !!v;
      }).map(function(v) {
        return [v];
      });
    }

    if(typeof callback !== 'function') {
      callback = function() {};
    }

    let start = new Date().valueOf();

    if (this.pool._isFull()) {
      return callback(new Error('Pool is full'));
    }

    this.pool.connect((err, client, complete) => {

      if (err) {
        this.db.error(err.message);
        callback(err);
        return complete();
      }

      let queries = preparedArray.map(queryData => {

        let query = queryData[0];
        let params = queryData[1] || [];

        return (callback) => {
          this.db.log(query, params, new Date().valueOf() - start);
          client.query(queryData[0], queryData[1], callback);
        };

      });

      queries = [].concat(
        (callback) => {
          client.query('BEGIN', callback);
        },
        queries
      );

      this.db.info('Transaction started...');

      async.series(queries, (txnErr, results) => {

        if (txnErr) {

          this.db.error(txnErr.message);
          this.db.info('Rollback started...');

          client.query('ROLLBACK', (err) => {

            if (err) {
              this.db.error(`Rollback failed - ${err.message}`);
              this.db.info('Transaction complete!');
              callback(err);
            } else {
              this.db.info('Rollback complete!');
              this.db.info('Transaction complete!');
              callback(txnErr);
            }

            complete();

          });

        } else {

          this.db.info('Commit started...');

          client.query('COMMIT', (err) => {

            if (err) {
              this.db.error(`Commit failed - ${err.message}`);
              this.db.info('Transaction complete!');
              complete();
              callback(err);
              return;
            }

            this.db.info('Commit complete!');
            this.db.info('Transaction complete!');
            complete();
            callback(null, results);

          });

        }

      });

    });

  }

  /**
   * generating orderBy clause 
   * @param {String} table 
   * @param {Array} orderByArray 
   * @param {Array} groupByArray 
   * @param {Array} joinArray
   */
  generateOrderByClause(table, orderByArray, groupByArray, joinArray) {
    let columnEscapedOrderByArray = orderByArray.map(v => {
      v.escapedColumns = v.columnNames.map((columnName) => {
        let columnNameComponents = columnName.split('__');
        if (columnNameComponents.length === 1) {
          return `${this.escapeField(table)}.${this.escapeField(columnName)}`;
        } else if (joinArray) {
          let join;

          joinArray.every(joinObj => {
            join = joinObj.find(join => join.joinAlias === columnNameComponents.slice(0, -1).join('__'));
            return !join;
          });

          if (!join) {
            return `${this.escapeField(table)}.${this.escapeField(columnName)}`;
          }
          return `${this.escapeField(join.joinAlias)}.${this.escapeField(columnNameComponents[columnNameComponents.length - 1])}`
        } else {
          return null;
        }
      }).filter((columnName) => {
        return !!columnName;
      });
      return v;
    }).filter((v) => {
      return v.escapedColumns.length;
    });

    return !columnEscapedOrderByArray.length ? '' : ' ORDER BY ' + columnEscapedOrderByArray.map(v => {
      return `${(v.transformation || (v => v)).apply(null, v.escapedColumns)} ${v.direction}`;
    }).join(', ');

  }
}

module.exports = PostgresAdapter;
