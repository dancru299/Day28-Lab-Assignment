# Kaggle Notebook For Lab 28

This folder contains an uploadable Kaggle notebook for the Lab 28 GPU side.

## What to do

1. Go to Kaggle `Code` and create a new notebook.
2. Turn `Internet` on.
3. Turn `Accelerator` to `GPU`.
4. In the notebook menu, choose `File` -> `Import Notebook`.
5. Upload `kaggle/lab28_kaggle_bootstrap.ipynb`.
6. Open the imported notebook and edit only the `NGROK_AUTH_TOKEN` cell.
7. Run cells from top to bottom.

## Final notebook choice

- `kaggle/lab28_kaggle_bootstrap.ipynb`
  Use this for final submission. It starts real `vLLM` on Kaggle, exposes `/v1/...` through the Kaggle gateway, and serves `/embed` from sentence-transformers.
- `kaggle/lab28_kaggle_compat_server.ipynb`
  Keep this only as an emergency fallback while debugging. Do not use it as the final proof if your instructor requires real vLLM.

## What you will get

- `VLLM_NGROK_URL`
- `EMBED_NGROK_URL`

Those two values will be the same public base URL by design. The Kaggle gateway notebook exposes both `/v1/...` and `/embed` behind one ngrok tunnel to avoid the common free-ngrok issue where the second tunnel replaces the first one.

Paste those into `.env`, then run:

```bash
docker compose up -d --force-recreate api-gateway prefect-worker
python scripts/10_verify_kaggle_vllm.py
```

## Notes

- The notebook prints the ngrok URLs clearly at the end.
- The vLLM notebook defaults to `Qwen/Qwen2.5-0.5B-Instruct` because it is much safer on Kaggle T4 than the earlier 7B GPTQ model.
- Put the same model in `.env`: `MODEL_NAME=Qwen/Qwen2.5-0.5B-Instruct`.
- For final validation, keep `ALLOW_LLM_FALLBACK=false` so the API fails loudly if vLLM is not really reachable.
- If vLLM still fails, check the printed `vllm.log` tail in the notebook. The root cause appears there before `Engine core initialization failed`.
