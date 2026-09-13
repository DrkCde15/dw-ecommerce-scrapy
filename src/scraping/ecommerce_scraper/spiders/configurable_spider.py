"""
Spider generico configuravel via JSON/YAML.

Permite raspar qualquer site definindo seletores em um arquivo de configuracao.

Exemplo de uso:
    scrapy crawl configurable -a config=configs/books_toscrape.yml
    scrapy crawl configurable -a config=configs/mercadolivre.json
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import yaml
import scrapy
from itemloaders import ItemLoader
from itemloaders.processors import MapCompose, TakeFirst


def clean_price(value: str) -> float | None:
    """Remove caracteres nao numericos e converte para float."""
    if not value:
        return None
    import re
    cleaned = re.sub(r"[^\d.,]", "", value.strip())
    if not cleaned:
        return None
    # Detecta formato
    if "," in cleaned and "." in cleaned:
        last_comma = cleaned.rfind(",")
        last_dot = cleaned.rfind(".")
        if last_comma > last_dot:
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    elif "," in cleaned:
        cleaned = cleaned.replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


class ConfigurableItem(scrapy.Item):
    """Item dinamico baseado na configuracao."""
    fields = {}


class ConfigurableSpider(scrapy.Spider):
    """
    Spider generico que le seletores de um arquivo de configuracao.

    Uso:
        scrapy crawl configurable -a config=configs/example.yml
    """

    name = "configurable"

    custom_settings = {
        "ITEM_PIPELINES": {
            "src.scraping.ecommerce_scraper.pipelines.CleaningPipeline": 100,
            "src.scraping.ecommerce_scraper.pipelines.DuplicatesFilterPipeline": 200,
        },
    }

    def __init__(self, config=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not config:
            raise ValueError("Defina -a config=caminho/para/config.yml")

        self.config = self._load_config(config)
        self.allowed_domains = self.config.get("allowed_domains", [])

        start_url = self.config.get("start_url")
        if not start_url:
            raise ValueError("start_url obrigatorio na configuracao")
        self.start_urls = [start_url]

    def _load_config(self, config_path: str) -> dict:
        """Carrega configuracao de JSON ou YAML."""
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Config nao encontrada: {config_path}")

        with open(path, encoding="utf-8") as f:
            if path.suffix in (".yml", ".yaml"):
                return yaml.safe_load(f)
            else:
                return json.load(f)

    def parse(self, response):
        """Parse baseado nos seletores da configuracao."""
        pagination = response.meta.get("pagination", self.config.get("pagination", {}))
        selectors = self.config.get("selectors", {})
        item_selector = selectors.get("item")
        if not item_selector:
            self.logger.error("selectors.item obrigatorio na configuracao")
            return

        items = response.css(item_selector)
        self.logger.info(f"Encontrados {len(items)} itens com seletor: {item_selector}")

        fields = selectors.get("fields", {})

        for element in items:
            item = {}

            for field_name, field_config in fields.items():
                css_selector = field_config.get("css")
                attr = field_config.get("attr")
                processors = field_config.get("processors", [])

                if css_selector:
                    if attr:
                        value = element.css(f"{css_selector}::attr({attr})").get()
                    else:
                        values = element.css(f"{css_selector}::text").getall()
                        value = " ".join(values) if values else None

                    if value:
                        for proc_name in processors:
                            if proc_name == "clean_price":
                                value = clean_price(value)
                            elif proc_name == "strip":
                                value = value.strip()

                    item[field_name] = value

            item["source"] = response.url
            item["scraped_at"] = datetime.now(timezone.utc).isoformat()

            if any(v for k, v in item.items() if k not in ("source", "scraped_at")):
                yield item

        # Paginacao
        pagination = response.meta.get("pagination", {})
        next_selector = pagination.get("next")
        max_pages = pagination.get("max_pages", 1)

        if next_selector and max_pages > 1:
            next_url = response.css(next_selector).get()
            if next_url:
                pagination["max_pages"] = max_pages - 1
                yield response.follow(
                    next_url,
                    callback=self.parse,
                    meta={"pagination": pagination},
                )
