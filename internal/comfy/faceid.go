package comfy

import "strings"

// FaceID holds the knobs of the SDXL/SD1.5 FaceID graph used in the FaceSearch project
// (checkpoint, optional LoRA, IPAdapter FaceID on one face photo, KSampler, SaveImage).
type FaceID struct {
	Checkpoint     string  `json:"checkpoint"`
	LoRA           string  `json:"lora,omitempty"`
	LoRAStrength   float64 `json:"lora_strength,omitempty"`
	FaceImage      string  `json:"-"` // name returned by Upload
	Prompt         string  `json:"prompt"`
	NegativePrompt string  `json:"negative_prompt,omitempty"`
	Seed           int64   `json:"seed"`
	Width          int     `json:"width,omitempty"`
	Height         int     `json:"height,omitempty"`
	Steps          int     `json:"steps,omitempty"`
	CFG            float64 `json:"cfg,omitempty"`
	FaceWeight     float64 `json:"face_weight,omitempty"`
	Prefix         string  `json:"-"`
}

func (p FaceID) withDefaults() FaceID {
	def := func(v *int, d int) {
		if *v == 0 {
			*v = d
		}
	}
	deff := func(v *float64, d float64) {
		if *v == 0 {
			*v = d
		}
	}
	def(&p.Width, 640)
	def(&p.Height, 896)
	def(&p.Steps, 28)
	deff(&p.CFG, 5)
	deff(&p.FaceWeight, 0.85)
	deff(&p.LoRAStrength, 0.8)
	if strings.TrimSpace(p.NegativePrompt) == "" {
		p.NegativePrompt = "blurry, deformed face, extra fingers, lowres, watermark"
	}
	return p
}

// Graph builds the ComfyUI API-format prompt. Node ids match the Python builder it replaces.
func (p FaceID) Graph() map[string]any {
	p = p.withDefaults()
	n := map[string]any{
		"1": node("CheckpointLoaderSimple", map[string]any{"ckpt_name": p.Checkpoint}),
	}
	model, clip := []any{"1", 0}, []any{"1", 1}
	if p.LoRA != "" {
		n["2"] = node("LoraLoader", map[string]any{"model": model, "clip": clip, "lora_name": p.LoRA,
			"strength_model": p.LoRAStrength, "strength_clip": p.LoRAStrength})
		model, clip = []any{"2", 0}, []any{"2", 1}
	}
	n["3"] = node("LoadImage", map[string]any{"image": p.FaceImage, "upload": "image"})
	n["4"] = node("IPAdapterUnifiedLoaderFaceID", map[string]any{"model": model, "preset": "FACEID PLUS V2",
		"lora_strength": 0.6, "provider": "CPU"})
	n["17"] = node("CLIPVisionLoader", map[string]any{"clip_name": "CLIP-ViT-H-14-laion2B-s32B-b79K.safetensors"})
	n["5"] = node("IPAdapterFaceID", map[string]any{"model": []any{"4", 0}, "ipadapter": []any{"4", 1},
		"image": []any{"3", 0}, "clip_vision": []any{"17", 0}, "weight": p.FaceWeight, "weight_faceidv2": 1.0,
		"weight_type": "linear", "combine_embeds": "concat", "start_at": 0.0, "end_at": 1.0, "embeds_scaling": "V only"})
	n["6"] = node("CLIPTextEncode", map[string]any{"text": p.Prompt, "clip": clip})
	n["7"] = node("CLIPTextEncode", map[string]any{"text": p.NegativePrompt, "clip": clip})
	n["8"] = node("EmptyLatentImage", map[string]any{"width": p.Width, "height": p.Height, "batch_size": 1})
	n["9"] = node("KSampler", map[string]any{"seed": p.Seed, "steps": p.Steps, "cfg": p.CFG,
		"sampler_name": "dpmpp_2m", "scheduler": "karras", "denoise": 1.0, "model": []any{"5", 0},
		"positive": []any{"6", 0}, "negative": []any{"7", 0}, "latent_image": []any{"8", 0}})
	n["10"] = node("VAEDecode", map[string]any{"samples": []any{"9", 0}, "vae": []any{"1", 2}})
	n["11"] = node("SaveImage", map[string]any{"filename_prefix": p.Prefix, "images": []any{"10", 0}})
	return n
}

func node(class string, inputs map[string]any) map[string]any {
	return map[string]any{"class_type": class, "inputs": inputs}
}
