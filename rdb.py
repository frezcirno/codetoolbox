from typing import Iterable, List, Tuple, Union

# pip install mysql-connector-python
import mysql.connector
from loguru import logger


class DBConnection:
    def __init__(self, **kwargs) -> None:
        self.pool = mysql.connector.pooling.MySQLConnectionPool(pool_size=32, **kwargs)

    def get_conn(self):
        return self.pool.get_connection()

    def execute(self, sql, args=(), commit=True, ignore_error=False) -> List[Tuple]:
        assert isinstance(sql, str)
        assert isinstance(args, (tuple, dict))
        try:
            with self.get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, args)
                res = cursor.fetchall()
                if commit:
                    conn.commit()
            return res
        except Exception as e:
            logger.error("Error executing sql: {} with args: {}, err: {}", sql, args, e)
            if ignore_error:
                return []
            raise
        finally:
            pass

    def execute_yield(self, sql, args=(), commit=True, ignore_error=False):
        assert isinstance(sql, str)
        assert isinstance(args, (tuple, dict))
        try:
            with self.get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, args)
                yield from cursor
                if commit:
                    conn.commit()
        except Exception:
            logger.error(f"Error executing sql: {sql} with args: {args}")
            if ignore_error:
                return None
            raise
        finally:
            pass

    def executemany(self, sql, args=(), commit=True, ignore_error=False):
        assert isinstance(sql, str)
        assert isinstance(args, Iterable)
        try:
            with self.get_conn() as conn:
                cursor = conn.cursor()
                cursor.executemany(sql, args)  # type: ignore
                if commit:
                    conn.commit()
        except Exception:
            logger.error(f"Error executing sql: {sql} with args")
            if ignore_error:
                return None
            raise
        finally:
            pass

    def select(
        self,
        table: str,
        query: Union[str, dict],
        projection=None,
        skip=0,
        limit=None,
        order=None,
        ignore_error=False,
        yield_=False,
    ):
        args = ()
        if isinstance(query, dict):
            args = tuple(query.values())
            query = " AND ".join(["1=1"] + [f"{k} = %s" for k in query.keys()])

        sql = f"SELECT {projection or '*'} FROM {table} WHERE {query}"
        if order is not None:
            sql += f" ORDER BY {order}"
        if limit is not None:
            sql += f" LIMIT {skip}, {limit}"

        return (self.execute_yield if yield_ else self.execute)(
            sql,
            args,
            commit=False,
            ignore_error=ignore_error,
        )

    def insert(
        self,
        table: str,
        data: List[dict],
        commit=True,
        ignore_error=False,
    ):
        if not data:
            return None

        keys = data[0].keys()
        sql = f"INSERT INTO {table} ({', '.join(keys)}) VALUES ({', '.join(['%s'] * len(keys))})"
        args = [tuple(d.values()) for d in data]

        return self.executemany(
            sql,
            args,
            commit=commit,
            ignore_error=ignore_error,
        )

    def update(
        self,
        table: str,
        query: Union[str, dict],
        update: Union[str, dict],
        commit=True,
        ignore_error=False,
    ):
        args = ()
        if isinstance(update, dict):
            args += tuple(update.values())
            update = ", ".join([f"{k} = %s" for k in update.keys()])

        if isinstance(query, dict):
            args += tuple(query.values())
            query = " AND ".join(["1=1"] + [f"{k} = %s" for k in query.keys()])

        sql = f"UPDATE {table} SET {update} WHERE {query}"

        return self.execute(
            sql,
            args,
            commit=commit,
            ignore_error=ignore_error,
        )

    def delete(
        self,
        table: str,
        query: Union[str, dict],
        commit=True,
        ignore_error=False,
    ):
        args = ()
        if isinstance(query, dict):
            args = tuple(query.values())
            query = " AND ".join(["1=1"] + [f"{k} = %s" for k in query.keys()])

        sql = f"DELETE FROM {table} WHERE {query}"

        return self.execute(
            sql,
            args,
            commit=commit,
            ignore_error=ignore_error,
        )

    def count(
        self,
        table: str,
        query: Union[str, dict],
        commit=True,
        ignore_error=False,
    ) -> int:
        args = ()
        if isinstance(query, dict):
            args = tuple(query.values())
            query = " AND ".join(["1=1"] + [f"{k} = %s" for k in query.keys()])

        sql = f"SELECT COUNT(*) FROM {table} WHERE {query}"

        return self.execute(
            sql,
            args,
            commit=commit,
            ignore_error=ignore_error,
        )[0][0]
