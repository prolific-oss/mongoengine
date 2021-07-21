import threading
import unittest
from random import uniform
from time import sleep

from mongoengine import Document, IntField, OperationError, StringField
from mongoengine.context_managers import run_in_transaction
from tests.utils import MongoDBTestCase


class ConcurrencyTest(MongoDBTestCase):
    @staticmethod
    def test_multiple_threads_modifying_different_instances():
        doc1 = Article(title="doc1", views=0).save()
        doc2 = Article(title="doc2", views=0).save()
        doc3 = Article(title="doc3", views=0).save()
        doc4 = Article(title="doc4", views=0).save()

        run_threads(
            [
                threading.Thread(
                    target=doc1.increase_views,
                ),
                threading.Thread(
                    target=doc2.increase_views,
                    args=(True,),
                ),
                threading.Thread(
                    target=doc3.increase_views,
                ),
                threading.Thread(
                    target=doc4.increase_views,
                    args=(True,),
                ),
            ]
        )

        doc1.reload()
        doc2.reload()
        doc3.reload()
        doc4.reload()
        assert doc1.views == 1
        assert doc2.views == 0
        assert doc3.views == 1
        assert doc4.views == 0

    @staticmethod
    def test_multiple_threads_modifying_same_instance():
        doc1 = Article(title="doc1", views=0).save()
        expected_views = 80

        threads = []
        for _ in range(0, expected_views):
            threads += [
                threading.Thread(
                    target=doc1.increase_views,
                ),
                threading.Thread(
                    target=doc1.increase_views,
                    args=(True,),
                ),
            ]

        run_threads(threads)

        doc1.reload()
        assert doc1.views == expected_views


class CustomException(Exception):
    pass


class Article(Document):
    title = StringField()
    views = IntField()

    def increase_views(self, raise_exception: bool = False):
        try:
            with run_in_transaction():
                self.modify(inc__views=1)
                if raise_exception:
                    raise CustomException("test")
        except CustomException:
            # Do nothing
            pass
        except OperationError:
            # Retry on Operation Error
            sleep(uniform(0, 3))
            self.increase_views(raise_exception)


def run_threads(threads):
    # Start them all at the same time
    for thread in threads:
        thread.start()
    # Wait for all to complete
    for thread in threads:
        thread.join()


if __name__ == "__main__":
    unittest.main()
