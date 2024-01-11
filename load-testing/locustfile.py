import os

from locust import HttpUser, between, task


class ApiUser(HttpUser):
    wait_time = between(1, 2)

    def on_start(self):
        self.headers = {'X-API-KEY': os.environ['OFFSETS_DB_PRODUCTION_API_KEY']}

    @task
    def health_check(self):
        self.client.get('/health/', headers=self.headers)

    @task
    def get_projects(self):
        self.client.get('/projects/', headers=self.headers)

    @task
    def list_credits(self):
        self.client.get('/credits/', headers=self.headers)

    @task
    def projects_by_listing_date(self):
        self.client.get('/charts/projects_by_listing_date', headers=self.headers)

    @task
    def credits_by_transaction_date(self):
        self.client.get('/charts/credits_by_transaction_date', headers=self.headers)
