#Intro
This repository is both for research gathering and code production.

My ambition is to find a way to leverage javascript web workers in finding the median of image stacks.
The framework will be Map Reduce and a restfull api will serve the webworkers with tiles and in the end receive resulting tiles as well.

The idea is to leverage the intense computation with temporal denoising timelapse videos off to a distributed browser based worker army, kindof seti @ home with timelapse videos.

##API
The api will have to be able to:
* have overview of a growing input frame repository.
* upon requests : slice up frames in a sliding time window and pack them as tile_X, tile_Y, time_window_Z image stacks (arrays)
* deliver these stacks to the webworkers who do the heavy lifting of finding the median.
* receive the flattened resulting median image tile
* put together a frame of tiles.
* append frame to resulting frames and encode to video.


#links:
[video denoise algorithm, pdf](http://bit.kuas.edu.tw/~jihmsp/2014/vol5/JIH-MSP-2014-01-004.pdf)

[webworker map reduce with server written in GO](https://github.com/oryband/go-web-mapreduce)

[image processing wiht python scipy](http://prancer.physics.louisville.edu/astrowiki/index.php/Image_processing_with_Python_and_SciPy)
