t = db.find3;
t.drop();

for ( i=1; i<=50; i++)
    t.save( { a : i } );

//Validate before execution. Validate will fail if the limit cursor is still open (SERVER-17792)
assert(t.validate().valid);

assert.eq( 50 , t.find().toArray().length );
assert.eq( 20 , t.find().limit(20).toArray().length );