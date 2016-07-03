# Xenodermus

Xenodermus is a simple (only about 200 lines of code) file storage system.

# Why?

I had several needs I didn't feel were adequately fulfilled by existing
solutions. I need to store many files--hundreds of thousands or potentially
more--ranging in size from a few kilobytes to several gigabytes. Since the size
of the file store would only grow, I needed to be able to add storage space to
the pool in the future without disruption, and to have files spread around
several different storage locations, including local or networked drives, or
potentially web services.

The first obvious option is something like an NAS or using ZFS locally. But the
former requires the NAS to be always accessible to the software and both require
the user to perform some arcane rituals before the software can be used. Not
good enough.

The second option was to use some existing file storage tool. Some are
available for python:

* [DEPOT](http://depot.readthedocs.io/en/latest/) has some
  handy features, but its local file storage just dumps files in a single
  directory, which isn't ideal if you have very many files. GridFS would be
  totally suitable except that it requires MongoDB to be running.
* [OFS](https://pythonhosted.org/ofs/) has PairTree-based local storage, which
  solves the problem of storing too many files, but it still doesn't allow files
  to be stored in a variety of locations in a single file store.

The third option was to go far afield and use something entirely different, like
[IPFS](https://ipfs.io/), for file storage. I did actually try this, but it
wasn't feasible. The IPFS server wasn't stable and the
[python API](https://github.com/ipfs/py-ipfs-api) isn't very good. You can't
even store data from an open filehandle with it--you have to save a temporary
file and then insert that. So, IPFS was out.

# What does it do, then?

Fundamentally, it just stores files. Here's a complete, from-zero example:

```python
>>> import xenodermus as xeno
>>> h = xeno.Hoard()
>>> with open('test.jpg', 'rb') as f:
...     h.put(f, 'test.jpg')
1 # the file id
>>> with h.get(1) as f:
...     with open('output.jpg', 'wb') as w:
...         w.write(f.read())
577106
```

You store a file in the Hoard and get back the file's ID. Then, you get the file
and read the data back when you need it. In its default configuration, shown
above, xenodermus will create a single Hoard and a single Store, and place both
in the current directory.

Under the hood, files are split into chunks (by default at most 256KB per chunk)
for storage and then intelligently reassembled as you read from them. You can
have multiple file stores, and the chunks will be distributed among them.

# More details

## Per-Store weighting

Each Store can be given a custom weight so that file chunks will be assigned to
some Stores at a higher rate than others. So if you have Store A on a drive with
50GB of free space, and Store B on a drive with 100GB of free space, you can
assign double the weight to Store B and the free space will fill at
approximately the same rate, proportionally. Or you could assign zero weight to
a Store and it will continue serving read requests, but no new chunks will be
stored on it.

## Multiple Stores, multiple Hoards

A Hoard is a collection of files stored on one or more Stores. The Hoard keeps
track of what files are stored, which chunks belong to which files, and which
Store each chunk resides in.

A Store, by contrast, is very dumb: it's essentially an interface on some kind
of file storage that can save a chunk of data and retrieve it upon request. It
doesn't even know what chunks it's storing, because it doesn't need to. So, a
single Store could potentially serve multiple Hoards, if desired, just as a
single Hoard could store its files on several Stores.

A Hoard just requires a directory of its own to store its database and
configuration file, plus a collection of Stores to use. A single application
could instantiate many Hoards and many Stores and use them in any arrangement
desired without any interference. Hoards are self-contained and Stores don't
in general have any state.

## Backing things up

Backing up a xenodermus Hoard is simple: the Hoard has just a configuration
file and a SQLite database, and the local file Store has just a configuration
file and a folder full of (a hierarchy of folders full of) chunks. Just back up
these files and you're done.
