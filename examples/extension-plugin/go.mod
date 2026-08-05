// OUT-OF-TREE EXAMPLE: a separate Go module that adds custom plugins to
// flogo-datapipeline using only the public `extension` contract — with no change
// to the engine itself.
//
// In a real project you would depend on a tagged release and drop the replace:
//
//	require github.com/mpandav-tibco/flogo-datapipeline vX.Y.Z
//
// The replace below just points this example at the repo it lives in.
module example.com/acme-flogo-plugins

go 1.25.7

require github.com/mpandav-tibco/flogo-datapipeline v0.0.0

replace github.com/mpandav-tibco/flogo-datapipeline => ../..
