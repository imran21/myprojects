 var ss = require('socket.io-stream');
 
 
 io.of('/user').on('connection', function(stream) {
 console.log('file user');
   ss(stream).on('send-file', function(stream, data) {
        var filename = path.basename(data.name);
		console.log(filename);
        stream.pipe(fs.createWriteStream(filename));
    });
});
