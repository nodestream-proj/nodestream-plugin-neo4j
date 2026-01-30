from typing import Any

from hamcrest.core.base_matcher import BaseMatcher

from nodestream_plugin_neo4j.query import Query


class RanQueryOnShot(BaseMatcher):
    def __init__(self, query: Query):
        self.query = query

    def search_mock(self, mock) -> bool:
        expected_stmt = self.query.query_statement
        expected_params = self.query.parameters
        for c in mock.await_args_list:
            if not c.args:
                continue
            actual = c.args[0]
            if (
                getattr(actual, "query_statement", None) == expected_stmt
                and getattr(actual, "parameters", None) == expected_params
            ):
                return True
        return False

    def _matches(self, item: Any) -> bool:
        return self.search_mock(item.database_connection.execute)

    def describe_to(self, description):
        description.append_text("query: ").append_text(self.query)


def ran_query(query: Query):
    return RanQueryOnShot(query)
