import sys


def update_progress(progress, finished):
    barlength = 100  # Modify this to change the length of the progress bar
    status = ""
    if isinstance(progress, int):
        progress = float(progress)
    if not isinstance(progress, float):
        progress = 0
        status = "error: progress var must be float\r\n"
    if progress < 0:
        progress = 0
        status = "Halt...\r\n"
    if progress >= 1:
        progress = 1
        status = "Done...\r\n"
    block = int(round(barlength*progress))
    text = "\rPercent: [{0}] {1}% {2} ({3} files completed)".format("#"*block + "-"*(barlength-block), progress*100, status, finished)
    sys.stdout.write(text)
    sys.stdout.flush()