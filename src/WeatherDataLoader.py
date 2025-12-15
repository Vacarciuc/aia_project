from pathlib import Path
import pandas as pd


class WeatherDataLoader:
    def __init__(self, root_path: str, file_names: list[str]):
        self.root_path = Path(root_path)
        self.file_names = file_names

    def get_subfolders(self) -> list[Path]:
        return [
            folder for folder in self.root_path.iterdir()
            if folder.is_dir()
        ]

    def load_files_from_all_folders(self) -> pd.DataFrame:

        data_frames = []

        for folder in self.get_subfolders():
            year = folder.name  # ex: 1990

            for file_name in self.file_names:
                file_path = folder / f"{file_name}.csv"

                if not file_path.exists():
                    print(f"[WARN] Lipsă fișier: {file_path}")
                    continue

                try:
                    df = pd.read_csv(file_path)
                    df["year"] = int(year)
                    df["source_file"] = file_name
                    data_frames.append(df)

                except Exception as e:
                    print(f"[ERROR] {file_path}: {e}")

        if not data_frames:
            raise ValueError("Nu s-au încărcat date!")

        return pd.concat(data_frames, ignore_index=True)
