import asyncio
import secrets
from abc import ABC, abstractmethod
from io import BytesIO

import boto3
import boto3.s3


class StorageError(Exception):
    pass


class BaseStorageBackend(ABC):
    @abstractmethod
    async def get_object(self, key: str) -> BytesIO:
        pass

    async def put_object(self, obj: BytesIO, filename: str) -> None:
        key = self.generate_object_key(filename)
        await self.perform_put_object(obj, key)
        await self.post_put_object(key)

    def generate_object_key(self, filename: str) -> str:
        return secrets.token_hex(16) + '-' + filename

    @abstractmethod
    async def perform_put_object(self, obj: BytesIO, key: str) -> None:
        pass

    async def post_put_object(self, key: str) -> None:
        pass

    async def delete_object(self, key: str) -> None:
        await self.perform_delete_object(key)
        await self.post_delete_object(key)

    @abstractmethod
    async def perform_delete_object(self, key: str) -> None:
        pass

    async def post_delete_object(self, key: str) -> None:
        pass


class S3StorageBackend(BaseStorageBackend):
    def __init__(self, connection_kwargs: dict[str, str], bucket_name: str):
        self.client = boto3.client("s3", **connection_kwargs)
        self.bucket_name = bucket_name

    async def get_object(self, key: str) -> BytesIO:
        obj = await asyncio.to_thread(
            self._get_object_sync,
            key=key,
        )
        return obj

    def _get_object_sync(self, key: str) -> BytesIO:
        try:
            response = self.client.get_object(Bucket=self.bucket_name, Key=key)
        except self.client.exceptions.NoSuchKey:
            raise FileNotFoundError
        except self.client.exceptions.ClientError as e:
            raise StorageError(f'Error while getting object (key: {key}). Error: {e}')

        return BytesIO(response["Body"].read())

    async def perform_put_object(self, obj: BytesIO, key: str) -> None:
        try:
            await asyncio.to_thread(self.client.put_object, Bucket=self.bucket_name, Key=key, Body=obj)
        except self.client.exceptions.ClientError as e:
            raise StorageError(f'Error while putting object (key: {key}). Error: {e}')

    async def perform_delete_object(self, key: str) -> None:
        try:
            await asyncio.to_thread(self.client.delete_object, Bucket=self.bucket_name, Key=key)
        except self.client.exceptions.ClientError as e:
            raise StorageError(f'Error while deleting object (key: {key}). Error: {e}')
