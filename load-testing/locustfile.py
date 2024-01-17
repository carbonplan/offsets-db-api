import os
import sys

from locust import FastHttpUser, between, events, task


class ApiUser(FastHttpUser):
    wait_time = between(1, 2)

    def on_start(self):
        self.headers = {'X-API-KEY': os.environ['OFFSETS_DB_API_KEY']}

    @task
    def get_projects(self):
        self.client.get(
            '/projects/?sort=-issued&current_page=1&per_page=100&registry=verra&registry=gold-standard&registry=global-carbon-council&registry=american-carbon-registry&registry=climate-action-reserve&registry=art-trees&category=agriculture&category=biochar&category=cookstove&category=energy-efficiency&category=forest&category=fuel-switching&category=ghg-management&category=land-use&category=mine-methane&category=renewable-energy&category=transportation&category=unknown',
            headers=self.headers,
            debug=sys.stderr,
        )

    @task
    def get_clips(self):
        self.client.get(
            '/clips/?current_page=1&per_page=200&sort=-date', headers=self.headers, debug=sys.stderr
        )

    @task
    def credits_by_category(self):
        self.client.get(
            '/charts/credits_by_category/?registry=verra&registry=gold-standard&registry=global-carbon-council&registry=american-carbon-registry&registry=climate-action-reserve&registry=art-trees&category=agriculture&category=biochar&category=cookstove&category=energy-efficiency&category=forest&category=fuel-switching&category=ghg-management&category=land-use&category=mine-methane&category=renewable-energy&category=transportation&category=unknown',
            headers=self.headers,
            debug=sys.stderr,
        )

    @task
    def credits_by_transaction_date(self):
        self.client.get(
            '/charts/credits_by_transaction_date/VCS902?freq=Y&current_page=1&per_page=200',
            headers=self.headers,
            debug=sys.stderr,
        )


@events.quitting.add_listener
def _(environment, **kw):
    environment.process_exit_code = 0
