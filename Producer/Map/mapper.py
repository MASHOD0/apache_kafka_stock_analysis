import dask.dataframe as dd

class Mapper():
    def __init__(self, file):
        self.file = file

    def read_file(self):
        ddf = dd.read_csv(self.file)
        return ddf
    
    def map_function(self, batch_df):
        # Extract relevant information from the batch
        date = batch_df['Date']
        close_price = batch_df['Close']
        volume = batch_df['Volume']

        # Emit intermediate key-value pairs
        return (date.compute().iloc[0], {
            'mean_close': close_price.mean().compute(),
            'min_close': close_price.min().compute(),
            'max_close': close_price.max().compute(),
            'std_close': close_price.std().compute(),
            'mean_volume': volume.mean().compute()
        })
