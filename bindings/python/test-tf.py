import sys

memory = False
if '-m' in sys.argv:
    memory = True
    sys.argv.remove('-m')
file = False
if '-f' in sys.argv:
    file = True
    sys.argv.remove('-f')
if not file and not memory:
    print("Neither -m, nor -f specified. Forcing -f")
    file = True

import lowfive
vol = lowfive.create_MetadataVOL()
if file:
    vol.set_passthru("*","*")
if memory:
    vol.set_memory("*","*")
vol.set_keep(True)

import tensorflow as tf
import argparse, socket, os, timeit

class Logger():
    def __init__(self, path, rank = 0):
        self.rank = rank
        os.makedirs(path, exist_ok = True)
        self.fn = open(os.path.join(path, "node-%s-%d.log" % (socket.gethostname(), self.rank)), 'w')
        self.ts = timeit.default_timer()

    def log(self, line, ref=0.0):
        now = timeit.default_timer()
        if ref == 0.0:
            print("[%3.f] Rank %d: %s" % (now - self.ts, self.rank, line), file = self.fn)
        else:
            print("[%3.f] [duration: %.3f] Rank %d: %s" % (now - self.ts, now - ref, self.rank, line), file = self.fn)
        self.fn.flush()

def build_model(args):
    # Start with a standard ResNet50 model
    model = tf.keras.applications.ResNet152(include_top=True, weights=None, classes=200)

    # The ResNet failimy shipped with Keras is optimized for inference.
    # Add L2 weight decay & adjust BN settings.
    model_config = model.get_config()
    for layer, layer_config in zip(model.layers, model_config['layers']):
        if hasattr(layer, 'kernel_regularizer'):
            regularizer = tf.keras.regularizers.l2(args.wd)
            layer_config['config']['kernel_regularizer'] = \
                {'class_name': regularizer.__class__.__name__,
                 'config': regularizer.get_config()}
        if type(layer) == tf.keras.layers.BatchNormalization:
            layer_config['config']['momentum'] = 0.9
            layer_config['config']['epsilon'] = 1e-5

    model = tf.keras.models.Model.from_config(model_config)
    opt = tf.keras.optimizers.SGD(learning_rate=args.base_lr, momentum=args.momentum)
    model.compile(loss=tf.keras.losses.categorical_crossentropy,
                  optimizer=opt,
                  metrics=['accuracy'])
    return model

def save_model(model, args):
    model.save_weights(args.model_path + '/resnet.model.%s' % args.save_fmt, save_format=args.save_fmt)

def load_model(model, args):
    model.load_weights(args.model_path + '/resnet.model.%s' % args.save_fmt)
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Keras ResNet50',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--model-path', default="./", help='path to model')
    parser.add_argument('--save-fmt', default="h5", help='save format')
    parser.add_argument('--base-lr', type=float, default=0.0125, help='learning rate for a single GPU')
    parser.add_argument('--momentum', type=float, default=0.9, help='SGD momentum')
    parser.add_argument('--wd', type=float, default=0.00005, help='weight decay')
    args = parser.parse_args()

    logger = Logger(args.model_path)
    start = timeit.default_timer()
    model = build_model(args)
    logger.log("built model", start)
    now = timeit.default_timer()
    save_model(model, args)
    logger.log("saved model using format %s" % args.save_fmt, now)
    now = timeit.default_timer()
    load_model(model, args)
    logger.log("loaded back model using format %s" % args.save_fmt, now)
    logger.log("test completed", start)
