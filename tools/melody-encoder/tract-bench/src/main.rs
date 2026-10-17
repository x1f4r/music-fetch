//! Load the encoder ONNX with tract (as mfcore loads SwiftF0), check it against reference
//! outputs from onnxruntime, and time it on one thread.
//!
//! Usage: tract-bench <encoder.onnx> <features.bin> <embeddings.bin> <batch>
//! features.bin: f32 LE (batch, 3, 160); embeddings.bin: f32 LE (batch, 128).

use std::time::Instant;

use tract_onnx::prelude::*;

fn read_f32(path: &str) -> Vec<f32> {
    std::fs::read(path).unwrap().chunks_exact(4).map(|b| f32::from_le_bytes([b[0], b[1], b[2], b[3]])).collect()
}

fn main() -> TractResult<()> {
    let args: Vec<String> = std::env::args().collect();
    let (onnx, feats, embs, batch) = (&args[1], &args[2], &args[3], args[4].parse::<usize>().unwrap());
    let t = Instant::now();
    let mut model = tract_onnx::onnx().model_for_path(onnx)?;
    let b = model.symbols.sym("B");
    model.set_input_fact(0, f32::fact(&[b.to_dim(), 3.to_dim(), 160.to_dim()]).into())?;
    let plan = model.into_optimized()?.into_runnable()?;
    println!("load + optimize: {:.1} ms", t.elapsed().as_secs_f64() * 1e3);
    let x = read_f32(feats);
    let want = read_f32(embs);
    let input = Tensor::from_shape(&[batch, 3, 160], &x)?;
    let out = plan.run(tvec!(input.clone().into()))?;
    let got = out[0].to_plain_array_view::<f32>()?;
    let maxdiff = got.iter().zip(&want).map(|(a, b)| (a - b).abs()).fold(0f32, f32::max);
    println!("output {:?}, max abs diff vs onnxruntime {maxdiff:.2e}", got.shape());
    for n in [1usize, 4, 8] {
        let n = n.min(batch);
        let xs = Tensor::from_shape(&[n, 3, 160], &x[..n * 3 * 160])?;
        plan.run(tvec!(xs.clone().into()))?;
        let reps = 50;
        let t = Instant::now();
        for _ in 0..reps {
            plan.run(tvec!(xs.clone().into()))?;
        }
        let dt = t.elapsed().as_secs_f64() / reps as f64;
        println!("tract batch {n}: {:.2} ms ({:.2} ms/window)", dt * 1e3, dt * 1e3 / n as f64);
    }
    Ok(())
}
