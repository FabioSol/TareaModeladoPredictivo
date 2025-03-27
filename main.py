from src.cleaner import CleaningPipeline
from src.pipeline import DataPipeline


if __name__ == "__main__":
    DataPipeline().run(download=False,parse=False)
