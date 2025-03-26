from src.pipeline import DataPipeline


if __name__ == "__main__":
    pipeline = DataPipeline()
    results = pipeline.run()
    print(f"Pipeline ran in {results['total_time']} seconds")